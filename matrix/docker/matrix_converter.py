"""Script to convert the outputs of Redshift queries into different formats."""

import argparse
import gzip
import json
import os
import shutil
import sys
import zipfile

import loompy
import pandas
import s3fs

from matrix.common import constants
from matrix.common import date
from matrix.common.logging import Logging
from matrix.common.constants import MatrixFormat
from matrix.common.request.request_tracker import RequestTracker, Subtask

LOGGER = Logging.get_logger(__file__)
SUPPORTED_FORMATS = [item.value for item in MatrixFormat]


class MatrixConverter:

    def __init__(self, args):
        self.args = args
        self.format = args.format
        self.request_tracker = RequestTracker(args.request_id)
        self.expression_manifest = None
        self.cell_manifest = None
        self.gene_manifest = None

        self.local_output_filename = os.path.basename(os.path.normpath(args.target_path))
        self.target_path = args.target_path
        self.working_dir = args.working_dir
        self.FS = s3fs.S3FileSystem()

        Logging.set_correlation_id(LOGGER, value=args.request_id)

    def run(self):
        try:
            LOGGER.debug(f"Beginning matrix conversion run for {self.args.request_id}")
            self.expression_manifest = self._parse_manifest(self.args.expression_manifest_key)
            self.cell_manifest = self._parse_manifest(self.args.cell_metadata_manifest_key)
            self.gene_manifest = self._parse_manifest(self.args.gene_metadata_manifest_key)

            LOGGER.debug(f"Beginning conversion to {self.format}")
            local_converted_path = getattr(self, f"_to_{self.format}")()
            LOGGER.debug(f"Conversion to {self.format} completed")

            LOGGER.debug(f"Beginning upload to S3")
            self._upload_converted_matrix(local_converted_path, self.target_path)
            LOGGER.debug("Upload to S3 complete, job finished")

            os.remove(local_converted_path)

            self.request_tracker.complete_subtask_execution(Subtask.CONVERTER)
            self.request_tracker.complete_request(duration=(date.get_datetime_now() -
                                                  date.to_datetime(self.request_tracker.creation_date))
                                                  .total_seconds())
        except Exception as e:
            LOGGER.info(f"Matrix Conversion failed on {self.args.request_id} with error {str(e)}")
            self.request_tracker.log_error(str(e))
            raise e

    def _parse_manifest(self, manifest_key):
        """Parse a manifest file produced by a Redshift UNLOAD query.

        Args:
            manifest_key: S3 location of the manifest file.

        Returns:
            dict with three keys:
                "columns": the column headers for the tables
                "part_urls": full S3 urls for the files containing results from each
                    Redshift slice
                "record_count": total number of records returned by the query
        """
        manifest = json.load(self.FS.open(manifest_key))

        return {
            "columns": [e["name"] for e in manifest["schema"]["elements"]],
            "part_urls": [e["url"] for e in manifest["entries"] if e["meta"]["record_count"]],
            "record_count": manifest["meta"]["record_count"]
        }

    def _n_slices(self):
        """Return the number of slices associated with this Redshift result.

        Redshift UNLOAD creates on object per "slice" of the cluster. We might want to
        iterate over that, so this get the count of them.
        """
        return len(self.cell_manifest["part_urls"])

    def _load_cell_table_slice(self, slice_idx):
        """Load the cell metadata table from a particular result slice.

        Args:
            slice_idx: Index of the slice to get cell metadata for

        Returns:
            dataframe of cell metadata. Index is "cellkey" and other columns are metadata
            fields.
        """

        cell_table_columns = self._map_columns(self.cell_manifest["columns"])
        cell_table_dtype = {c: "category" for c in cell_table_columns}
        cell_table_dtype["genes_detected"] = "uint32"
        cell_table_dtype["total_umis"] = "uint32"
        cell_table_dtype["emptydrops_is_cell"] = "bool"
        cell_table_dtype["cellkey"] = "object"

        part_url = self.cell_manifest["part_urls"][slice_idx]
        df = pandas.read_csv(
            part_url, sep='|', header=None, names=cell_table_columns,
            dtype=cell_table_dtype, true_values=["t"], false_values=["f"],
            index_col="cellkey")

        return df

    def _load_gene_table(self):
        """Load the gene metadata table.

        Returns:
            dataframe of gene metadata. Index is "featurekey"
        """

        gene_table_columns = self._map_columns(self.gene_manifest["columns"])

        dfs = []
        for part_url in self.gene_manifest["part_urls"]:
            df = pandas.read_csv(part_url, sep='|', header=None, names=gene_table_columns,
                                 true_values=["t"], false_values=["f"],
                                 index_col="featurekey")

            dfs.append(df)
        return pandas.concat(dfs)

    def _load_expression_table_slice(self, slice_idx, chunksize=1000000):
        """Load expression data from a slice, yielding the data by a fixed number
        of rows.

        Args:
            slice_idx: Index of the slice to get data for
            chunksize: Number of rows to yield at once

        Yields:
            dataframe of expression data
        """
        part_url = self.expression_manifest["part_urls"][slice_idx]
        expression_table_columns = ["cellkey", "featurekey", "exprvalue"]
        expression_dtype = {"cellkey": "object", "featurekey": "object", "exprvalue": "float32"}

        # Iterate over chunks of the remote file. We have to set a fixed set
        # number of rows to read, but we also want to make sure that all the
        # rows from a given cell are yielded with each chunk. So we are going
        # to keep track of the "remainder", rows from the end of a chunk for a
        # cell the spans a chunk boundary.
        remainder = None
        for chunk in pandas.read_csv(
                part_url, sep="|", names=expression_table_columns,
                dtype=expression_dtype, header=None, chunksize=chunksize):

            # If we have some rows from the previous chunk, prepend them to
            # this one
            if remainder is not None:
                adjusted_chunk = pandas.concat([remainder, chunk], axis=0, copy=False)
            else:
                adjusted_chunk = chunk

            # Now get the rows for the cell at the end of this chunk that spans
            # the boundary. Remove them from the chunk we yield, but keep them
            # in the remainder.
            last_cellkey = adjusted_chunk.tail(1).cellkey.values[0]
            remainder = adjusted_chunk.loc[adjusted_chunk['cellkey'] == last_cellkey]
            adjusted_chunk = adjusted_chunk[adjusted_chunk.cellkey != last_cellkey]

            yield adjusted_chunk

        if remainder is not None:
            yield remainder

    def _map_columns(self, cols):
        return [constants.TABLE_COLUMN_TO_METADATA_FIELD[col]
                if col in constants.TABLE_COLUMN_TO_METADATA_FIELD else col
                for col in cols]

    def _make_directory(self):
        if not self.local_output_filename.endswith(".zip"):
            self.local_output_filename += ".zip"
        results_dir = os.path.join(self.working_dir,
                                   os.path.splitext(self.local_output_filename)[0])
        os.makedirs(results_dir)
        return results_dir

    def _zip_up_matrix_output(self, results_dir, matrix_file_names):
        zipf = zipfile.ZipFile(os.path.join(self.working_dir, self.local_output_filename), 'w')
        for filename in matrix_file_names:
            zipf.write(os.path.join(results_dir, filename),
                       arcname=os.path.join(os.path.basename(results_dir),
                       filename))
        zipf.close()
        shutil.rmtree(results_dir)
        return os.path.join(self.working_dir, self.local_output_filename)

    def _write_out_gene_dataframe(self, results_dir, output_filename, compression=False):
        gene_df = self._load_gene_table()
        if compression:
            gene_df.to_csv(os.path.join(results_dir, output_filename),
                           index_label="featurekey",
                           sep="\t", compression="gzip")
        else:
            gene_df.to_csv(os.path.join(results_dir, output_filename), index_label="featurekey")
        return gene_df

    def _create_cell_dataframe(self):
        cell_df = pandas.concat([self._load_cell_table_slice(s) for s in range(self._n_slices())], copy=False)
        return cell_df

    def _write_out_cell_dataframe(self, results_dir, output_filename, cell_df, cellkeys, compression=False):
        cell_df = cell_df.reindex(index=cellkeys)
        if compression:
            cell_df.to_csv(os.path.join(results_dir, output_filename),
                           sep='\t',
                           index_label="cellkey", compression="gzip")
        else:
            cell_df.to_csv(os.path.join(results_dir, output_filename), index_label="cellkey")
        return cell_df

    def _to_mtx(self):
        """Write a zip file with an mtx and two metadata tsvs from Redshift query
        manifests.

        Returns:
           output_path: Path to the zip file.
        """
        results_dir = self._make_directory()
        gene_df = self._write_out_gene_dataframe(results_dir, "genes.tsv.gz", compression=True)
        cell_df = self._create_cell_dataframe()

        # To follow 10x conventions, features are rows and cells are columns
        n_rows = gene_df.shape[0]
        n_cols = cell_df.shape[0]
        n_nonzero = self.expression_manifest["record_count"]

        cellkeys = []

        with gzip.open(os.path.join(results_dir, "matrix.mtx.gz"), "w", compresslevel=4) as exp_f:
            # Write the mtx header
            exp_f.write("%%MatrixMarket matrix coordinate real general\n".encode())
            exp_f.write(f"{n_rows} {n_cols} {n_nonzero}\n".encode())

            cell_count = 0
            for slice_idx in range(self._n_slices()):
                for chunk in self._load_expression_table_slice(slice_idx):

                    grouped = chunk.groupby("cellkey")
                    for cell_group in grouped:
                        single_cell_df = cell_group[1]
                        single_cell_coo = single_cell_df.pivot(
                            index="featurekey", columns="cellkey", values="exprvalue").reindex(
                                index=gene_df.index).to_sparse().to_coo()

                        for row, col, value in zip(single_cell_coo.row, single_cell_coo.col, single_cell_coo.data):
                            exp_f.write(f"{row+1} {col+cell_count+1} {value}\n".encode())
                        cell_count += 1

                        cellkeys.append(cell_group[0])

        self._write_out_cell_dataframe(results_dir, "cells.tsv.gz", cell_df, cellkeys, compression=True)
        file_names = ["genes.tsv.gz", "matrix.mtx.gz", "cells.tsv.gz"]
        zip_path = self._zip_up_matrix_output(results_dir, file_names)
        return zip_path

    def _to_loom(self):
        """Write a loom file from Redshift query manifests.

        Returns:
           output_path: Path to the new loom file.
        """

        # Put loom on the output filename if it's not already there.
        if not self.local_output_filename.endswith(".loom"):
            self.local_output_filename += ".loom"

        # Read the row (gene) attributes and then set some conventional names
        gene_df = self._load_gene_table()
        gene_df["featurekey"] = gene_df.index
        row_attrs = gene_df.to_dict("series")
        # Not expected to be unique
        row_attrs["Gene"] = row_attrs.pop("featurename")
        row_attrs["Accession"] = row_attrs.pop("featurekey")
        for key, val in row_attrs.items():
            row_attrs[key] = val.values

        loom_parts = []
        loom_part_dir = os.path.join(self.working_dir, ".loom_parts")

        if os.path.exists(loom_part_dir):
            shutil.rmtree(loom_part_dir)

        os.makedirs(loom_part_dir)

        # Iterate over the "slices" produced by the redshift query
        for slice_idx in range(self._n_slices()):

            # Get the cell metadata for all the cells in this slice
            cell_df = self._load_cell_table_slice(slice_idx)

            # Iterate over fixed-size chunks of expression data from this
            # slice.
            chunk_idx = 0
            for chunk in self._load_expression_table_slice(slice_idx):
                print(f"Loading chunk {chunk_idx} from slice {slice_idx}")
                sparse_cell_dfs = []

                # Group the data by cellkey and iterate over each cell
                grouped = chunk.groupby("cellkey")
                for cell_group in grouped:
                    single_cell_df = cell_group[1]

                    # Reshape the dataframe so cellkey is a column and features
                    # are rows. Reindex so all dataframes have the same row
                    # order, and then sparsify because this is a very empty
                    # dataset usually.
                    sparse_cell_dfs.append(
                        single_cell_df.pivot(
                            index="featurekey", columns="cellkey", values="exprvalue")
                        .reindex(index=row_attrs["Accession"]).to_sparse())

                # Concatenate the cell dataframes together. This is what we'll
                # write to disk.
                if not sparse_cell_dfs:
                    continue
                sparse_expression_matrix = pandas.concat(sparse_cell_dfs, axis=1, copy=True)

                # Get the cell metadata dataframe for just the cell in this
                # chunk
                chunk_cell_df = cell_df.reindex(index=sparse_expression_matrix.columns)
                chunk_cell_df["cellkey"] = chunk_cell_df.index
                for col in chunk_cell_df.columns:
                    if chunk_cell_df[col].dtype.name == "category":
                        chunk_cell_df[col] = chunk_cell_df[col].astype("object")
                col_attrs = chunk_cell_df.to_dict("series")
                col_attrs["CellID"] = col_attrs.pop("cellkey")

                # Just a thing you have to do...
                for key, val in col_attrs.items():
                    col_attrs[key] = val.values

                # Write the data from this chunk to its own file.
                loom_part_path = os.path.join(loom_part_dir,
                                              f"matrix.{slice_idx}.{chunk_idx}.loom")
                print(f"Writing to {loom_part_path}")
                loompy.create(
                    loom_part_path, sparse_expression_matrix.to_coo(), row_attrs, col_attrs)
                loom_parts.append(loom_part_path)
                chunk_idx += 1

        # Using the loompy method, combine all the chunks together into a
        # single file.
        print(f"Parts complete. Writing to {self.local_output_filename}")
        loompy.combine(loom_parts,
                       key="Accession",
                       output_file=os.path.join(self.working_dir, self.local_output_filename))
        shutil.rmtree(loom_part_dir)

        return os.path.join(self.working_dir, self.local_output_filename)

    def _to_csv(self):
        """Write a zip file with csvs from Redshift query manifests and readme.

        Returns:
           output_path: Path to the new zip file.
        """

        results_dir = self._make_directory()
        gene_df = self._write_out_gene_dataframe(results_dir, "genes.csv")
        cell_df = self._create_cell_dataframe()

        cellkeys = []
        with open(os.path.join(results_dir, "expression.csv"), "w") as exp_f:
            # Write the CSV's header
            gene_index_string_list = [str(x) for x in gene_df.index.tolist()]
            exp_f.write(','.join(["cellkey"] + gene_index_string_list))
            exp_f.write('\n')

            for slice_idx in range(self._n_slices()):
                for chunk in self._load_expression_table_slice(slice_idx):
                    # Group the data by cellkey and iterate over each cell
                    grouped = chunk.groupby("cellkey")
                    for cell_group in grouped:
                        single_cell_df = cell_group[1]
                        single_cell_df.pivot(
                            index="cellkey", columns="featurekey", values="exprvalue").reindex(
                                columns=gene_df.index).to_csv(exp_f, header=False, na_rep='0')
                        cellkeys.append(cell_group[0])

        self._write_out_cell_dataframe(results_dir, "cells.csv", cell_df, cellkeys)
        file_names = ["genes.csv", "expression.csv", "cells.csv"]
        zip_path = self._zip_up_matrix_output(results_dir, file_names)
        return zip_path

    def _upload_converted_matrix(self, local_path, remote_path):
        """
        Upload the converted matrix to S3.
        Parameters
        ----------
        local_path : str
            Path to the new, converted matrix file
        remote_path : str
            S3 path where the converted matrix will be uploaded
        """
        self.FS.put(local_path, remote_path)


def main(args):
    """Entry point."""

    parser = argparse.ArgumentParser()
    parser.add_argument("request_id",
                        help="ID of the request associated with this conversion.")
    parser.add_argument("expression_manifest_key",
                        help="S3 url to Redshift manifest for the expression table.")
    parser.add_argument("cell_metadata_manifest_key",
                        help="S3 url to Redshift manifest for the cell table.")
    parser.add_argument("gene_metadata_manifest_key",
                        help="S3 url to Redshift manifest for the gene table.")
    parser.add_argument("target_path",
                        help="S3 prefix where the file should be written.")
    parser.add_argument("format",
                        help="Target format for conversion",
                        choices=SUPPORTED_FORMATS)
    parser.add_argument("working_dir",
                        help="Directory to write local files.")
    args = parser.parse_args(args)
    LOGGER.debug(
        f"Starting matrix conversion job with parameters: "
        f"{args.expression_manifest_key}, {args.cell_metadata_manifest_key}, "
        f"{args.gene_metadata_manifest_key}, {args.target_path}, {args.format}")

    matrix_converter = MatrixConverter(args)
    matrix_converter.run()

if __name__ == "__main__":
    print(f"STARTED with argv: {sys.argv}")
    main(sys.argv[1:])

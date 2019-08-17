"""Script to convert the outputs of Redshift queries into different formats."""

import argparse
import gzip
import os
import shutil
import sys
import zipfile

import loompy
import pandas
import s3fs

from matrix.common import date
from matrix.common.constants import MatrixFormat
from matrix.common.logging import Logging
from matrix.common.request.request_tracker import RequestTracker, Subtask
from matrix.common.query.cell_query_results_reader import CellQueryResultsReader
from matrix.common.query.expression_query_results_reader import ExpressionQueryResultsReader
from matrix.common.query.feature_query_results_reader import FeatureQueryResultsReader
from matrix.docker.query_runner import QueryType

LOGGER = Logging.get_logger(__file__)
SUPPORTED_FORMATS = [item.value for item in MatrixFormat]


class MatrixConverter:

    def __init__(self, args):
        self.args = args
        self.format = args.format
        self.request_tracker = RequestTracker(args.request_id)
        self.query_results = {}

        self.local_output_filename = os.path.basename(os.path.normpath(args.target_path))
        self.target_path = args.target_path
        self.working_dir = args.working_dir
        self.FS = s3fs.S3FileSystem()

        Logging.set_correlation_id(LOGGER, value=args.request_id)

    def run(self):
        try:
            LOGGER.debug(f"Beginning matrix conversion run for {self.args.request_id}")
            self.query_results = {
                QueryType.CELL: CellQueryResultsReader(self.args.cell_metadata_manifest_key),
                QueryType.EXPRESSION: ExpressionQueryResultsReader(self.args.expression_manifest_key),
                QueryType.FEATURE: FeatureQueryResultsReader(self.args.gene_metadata_manifest_key)
            }

            LOGGER.debug(f"Beginning conversion to {self.format}")
            local_converted_path = getattr(self, f"_to_{self.format}")()
            LOGGER.debug(f"Conversion to {self.format} completed")

            LOGGER.debug(f"Beginning upload to S3")
            self._upload_converted_matrix(local_converted_path, self.target_path)
            LOGGER.debug("Upload to S3 complete, job finished")

            os.remove(local_converted_path)

            self.request_tracker.complete_subtask_execution(Subtask.CONVERTER)
            self.request_tracker.complete_request(duration=(date.get_datetime_now()
                                                            - date.to_datetime(self.request_tracker.creation_date))
                                                  .total_seconds())
        except Exception as e:
            LOGGER.info(f"Matrix Conversion failed on {self.args.request_id} with error {str(e)}")
            self.request_tracker.log_error(str(e))
            raise e

    def _n_slices(self):
        """Return the number of slices associated with this Redshift result.

        Redshift UNLOAD creates on object per "slice" of the cluster. We might want to
        iterate over that, so this get the count of them.
        """
        return len(self.query_results[QueryType.CELL].manifest["part_urls"])

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
        gene_df = self.query_results[QueryType.FEATURE].load_results()
        if compression:
            gene_df.to_csv(os.path.join(results_dir, output_filename),
                           index_label="featurekey",
                           sep="\t", compression="gzip")
        else:
            gene_df.to_csv(os.path.join(results_dir, output_filename), index_label="featurekey")
        return gene_df

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
        cell_df = self.query_results[QueryType.CELL].load_results()

        # To follow 10x conventions, features are rows and cells are columns
        n_rows = gene_df.shape[0]
        n_cols = cell_df.shape[0]
        n_nonzero = self.query_results[QueryType.EXPRESSION].manifest["record_count"]

        cellkeys = []

        with gzip.open(os.path.join(results_dir, "matrix.mtx.gz"), "w", compresslevel=4) as exp_f:
            # Write the mtx header
            exp_f.write("%%MatrixMarket matrix coordinate real general\n".encode())
            exp_f.write(f"{n_rows} {n_cols} {n_nonzero}\n".encode())

            cell_count = 0
            for slice_idx in range(self._n_slices()):
                for chunk in self.query_results[QueryType.EXPRESSION].load_slice(slice_idx):

                    grouped = chunk.groupby("cellkey")
                    for cell_group in grouped:
                        single_cell_df = cell_group[1]
                        single_cell_coo = single_cell_df.pivot(
                            index="featurekey", columns="cellkey", values="exprvalue").reindex(
                            index=gene_df.index).to_sparse().to_coo()

                        for row, col, value in zip(single_cell_coo.row, single_cell_coo.col, single_cell_coo.data):
                            exp_f.write(f"{row + 1} {col + cell_count + 1} {value}\n".encode())
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
        gene_df = self.query_results[QueryType.FEATURE].load_results()
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
            cell_df = self.query_results[QueryType.CELL].load_slice(slice_idx)

            # Iterate over fixed-size chunks of expression data from this
            # slice.
            chunk_idx = 0
            for chunk in self.query_results[QueryType.EXPRESSION].load_slice(slice_idx):
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
                    sparse_cell_dfs.append(single_cell_df
                                           .pivot(index="featurekey", columns="cellkey", values="exprvalue")
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
        cell_df = self.query_results[QueryType.CELL].load_results()

        cellkeys = []
        with open(os.path.join(results_dir, "expression.csv"), "w") as exp_f:
            # Write the CSV's header
            gene_index_string_list = [str(x) for x in gene_df.index.tolist()]
            exp_f.write(','.join(["cellkey"] + gene_index_string_list))
            exp_f.write('\n')

            for slice_idx in range(self._n_slices()):
                for chunk in self.query_results[QueryType.EXPRESSION].load_slice(slice_idx):
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

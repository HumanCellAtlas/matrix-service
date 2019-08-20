"""Script to convert the outputs of Redshift queries into different formats."""

import argparse
import datetime
import gzip
import itertools
import os
import shutil
import sys
import zipfile

import h5py
import numpy
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

        def _grouper(iterable, n):
            args = [iter(iterable)] * n
            return itertools.zip_longest(*args, fillvalue=None)

        def _timestamp():
            return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")

        # Put loom on the output filename if it's not already there.
        if not self.local_output_filename.endswith(".loom"):
            self.local_output_filename += ".loom"

        # Read the row (gene) attributes and then set some conventional names
        gene_df = self.query_results[QueryType.FEATURE].load_results()
        gene_df["featurekey"] = gene_df.index

        gene_count = gene_df.shape[0]
        cell_count = self.query_results[QueryType.CELL]._parse_manifest()["record_count"]

        loom_path = os.path.join(self.working_dir, self.local_output_filename)
        loom_file = h5py.File(loom_path, mode="w")

        loom_file.attrs["CreationDate"] = _timestamp()
        loom_file.attrs["LOOM_SPEC_VERSION"] = "2.0.1"

        matrix_dataset = loom_file.create_dataset(
            "matrix",
            shape=(gene_count, cell_count),
            dtype="float32",
            compression="gzip",
            compression_opts=2,
            chunks=(gene_count, 1))

        cell_ids = []
        cell_counter = 0
        for slice_idx in range(self._n_slices()):
            for chunk in self.query_results[QueryType.EXPRESSION].load_slice(slice_idx):
                grouped = chunk.groupby("cellkey")

                for cell_group in _grouper(grouped, 50):
                    single_cells_df = pandas.concat((c[1] for c in cell_group if c), axis=0, copy=False)
                    pivoted = single_cells_df.pivot(
                        index="featurekey", columns="cellkey", values="exrpvalue").reindex(
                            index=gene_df.index, fill_value=0)
                    cell_ids.extend(pivoted.columns.to_list())
                    matrix_dataset[:, cell_counter:cell_counter + pivoted.shape[1]] = pivoted
                    cell_counter += pivoted.shape[1]
        matrix_dataset.attrs["last_modified"] = _timestamp()

        cell_df = self.query_results[QueryType.CELL].load_results().reindex(index=cell_ids)
        col_attrs_group = loom_file.create_group("col_attrs")
        cell_id_dset = col_attrs_group.create_dataset("CellID", data=cell_df.index, dtype=h5py.special_dtype(vlen=str),
                                                      compression='gzip', compression_opts=2, chunks=(256,))
        cell_id_dset.attrs["last_modified"] = _timestamp()

        for cell_metadata_field in cell_df:
            cell_metadata = cell_df[cell_metadata_field]
            if cell_metadata.dtype == numpy.dtype("O"):
                h5_dtype = h5py.special_dtype(vlen=str)
            else:
                h5_dtype = cell_metadata.dtype
            dset = col_attrs_group.create_dataset(cell_metadata_field, data=cell_metadata, dtype=h5_dtype,
                                                  compression='gzip', compression_opts=2, chunks=(256,))
            dset.attrs["last_modified"] = _timestamp()
        col_attrs_group.attrs["last_modified"] = _timestamp()

        row_attrs_group = loom_file.create_group("row_attrs")
        acc_dset = row_attrs_group.create_dataset("Accession", data=gene_df.index, dtype=h5py.special_dtype(vlen=str),
                                                  compression='gzip', compression_opts=2, chunks=(256,))
        acc_dset.attrs["last_modified"] = _timestamp()
        name_dset = row_attrs_group.create_dataset("Gene", data=gene_df["featurename"],
                                                   dtype=h5py.special_dtype(vlen=str),
                                                   compression='gzip', compression_opts=2, chunks=(256,))
        name_dset.attrs["last_modified"] = _timestamp()

        for gene_metadata_field in gene_df:
            if gene_metadata_field == "featurename":
                continue
            gene_metadata = gene_df[gene_metadata_field]
            if gene_metadata.dtype == numpy.dtype("O"):
                h5_dtype = h5py.special_dtype(vlen=str)
            else:
                h5_dtype = gene_metadata.dtype
            dset = row_attrs_group.create_dataset(gene_metadata_field, data=gene_metadata, dtype=h5_dtype,
                                                  compression='gzip', compression_opts=2, chunks=(256,))
            dset.attrs["last_modified"] = _timestamp()
        row_attrs_group.attrs["last_modified"] = _timestamp()

        loom_file.create_group("layers")
        loom_file.create_group("row_graphs")

        loom_file.attrs["last_modified"] = _timestamp()

        return loom_path

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

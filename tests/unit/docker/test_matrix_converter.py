import argparse
import datetime
import itertools
import os
import random
import shutil
import unittest
from unittest import mock

import pandas

from matrix.common import date
from matrix.common.request.request_tracker import Subtask
from matrix.common.query.cell_query_results_reader import CellQueryResultsReader
from matrix.common.query.expression_query_results_reader import ExpressionQueryResultsReader
from matrix.common.query.feature_query_results_reader import FeatureQueryResultsReader
from matrix.docker.matrix_converter import main, MatrixConverter, SUPPORTED_FORMATS
from matrix.docker.query_runner import QueryType


class TestMatrixConverter(unittest.TestCase):

    def setUp(self):
        self.test_manifest = {
            "columns": ["a", "b", "c"],
            "part_urls": ["A", "B", "C"],
            "record_count": 5
        }

        args = ["test_id", "test_exp_manifest", "test_cell_manifest",
                "test_gene_manifest", "test_target", "loom", "."]
        parser = argparse.ArgumentParser()
        parser.add_argument("request_id")
        parser.add_argument("expression_manifest_key")
        parser.add_argument("cell_metadata_manifest_key")
        parser.add_argument("gene_metadata_manifest_key")
        parser.add_argument("target_path")
        parser.add_argument("format", choices=SUPPORTED_FORMATS)
        parser.add_argument("working_dir")
        self.args = parser.parse_args(args)
        self.matrix_converter = MatrixConverter(self.args)

    @mock.patch("os.remove")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.creation_date", new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_request")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._upload_converted_matrix")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._to_loom")
    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_run(self,
                 mock_parse_manifest,
                 mock_to_loom,
                 mock_upload_converted_matrix,
                 mock_subtask_exec,
                 mock_complete_request,
                 mock_creation_date,
                 mock_os_remove):
        mock_parse_manifest.return_value = self.test_manifest
        mock_creation_date.return_value = date.to_string(datetime.datetime.utcnow())
        mock_to_loom.return_value = "local_matrix_path"

        self.matrix_converter.run()

        mock_manifest_calls = [
            mock.call("test_cell_manifest"),
            mock.call("test_exp_manifest"),
            mock.call("test_gene_manifest")
        ]
        mock_parse_manifest.assert_has_calls(mock_manifest_calls)
        mock_to_loom.assert_called_once()
        mock_subtask_exec.assert_called_once_with(Subtask.CONVERTER)
        mock_complete_request.assert_called_once()
        mock_upload_converted_matrix.assert_called_once_with("local_matrix_path", "test_target")

    @mock.patch("s3fs.S3FileSystem.open")
    def test__n_slices(self, mock_open):
        manifest_file_path = "tests/functional/res/cell_metadata_manifest"
        with open(manifest_file_path) as f:
            mock_open.return_value = f
            self.matrix_converter.query_results = {
                QueryType.CELL: CellQueryResultsReader("test_manifest_key")
            }

        self.assertEqual(self.matrix_converter._n_slices(), 8)

    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    @mock.patch("matrix.common.query.expression_query_results_reader.ExpressionQueryResultsReader.load_slice")
    def test__generate_expression_dfs(self, mock_load_slice, mock_parse_manifest):

        mock_parse_manifest.return_value = {
            "part_urls": ["url1"],
            "columns": ["cellkey", "featurekey", "exprvalue"],
            "record_count": 2624879
        }

        self.matrix_converter.query_results = {
            QueryType.CELL: CellQueryResultsReader("test_cell_manifest_key"),
            QueryType.EXPRESSION: ExpressionQueryResultsReader("test_expression_manifest_key")
        }

        # Create some fake gene and cell values. We'll have 2027 cells each
        # with 647 expressed genes. This makes sure the test hits some jagged
        # edges.
        genes = itertools.cycle(("gene_" + str(n) for n in range(647)))
        cells = itertools.chain.from_iterable((itertools.repeat("cell_" + str(n), 647) for n in range(2027)))

        full_expr_df = pandas.DataFrame(
            columns=["cellkey", "featurekey", "exprvalue"],
            data=[[c, f, random.randrange(1, 10000)] for c, f in zip(cells, genes)])
        # load_slice splits on 1000000 rows
        chunk1_df = full_expr_df[:999615]
        chunk2_df = full_expr_df[999615:]

        # Have load slice return two different chunks
        mock_load_slice.return_value = iter([chunk1_df, chunk2_df])

        # Keep track of how many unique cells we see and the sum of expression
        # values
        cell_counter = 0
        expr_sum = 0
        for cell_df in self.matrix_converter._generate_expression_dfs(50):
            num_cells = len(set(cell_df["cellkey"]))
            self.assertLessEqual(num_cells, 50)
            cell_counter += num_cells
            expr_sum += cell_df["exprvalue"].sum()

        # Verify we saw every cell and all the expression values
        self.assertEqual(cell_counter, 2027)
        self.assertEqual(expr_sum, full_expr_df["exprvalue"].sum())

    def test__make_directory(self):
        self.assertEqual(os.path.isdir('test_target'), False)
        results_dir = self.matrix_converter._make_directory()

        self.assertEqual(os.path.isdir('test_target'), True)
        shutil.rmtree(results_dir)

    def test__zip_up_matrix_output(self):
        results_dir = self.matrix_converter._make_directory()
        shutil.copyfile('LICENSE', './test_target/LICENSE')

        path = self.matrix_converter._zip_up_matrix_output(results_dir, ['LICENSE'])

        self.assertEqual(path, './test_target.zip')
        os.remove('./test_target.zip')

    @mock.patch("pandas.DataFrame.to_csv")
    @mock.patch("matrix.common.query.feature_query_results_reader.FeatureQueryResultsReader.load_results")
    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test__write_out_gene_dataframe__with_compression(self, mock_parse_manifest, mock_load_results, mock_to_csv):
        self.matrix_converter.query_results = {
            QueryType.FEATURE: FeatureQueryResultsReader("test_manifest_key")
        }
        results_dir = self.matrix_converter._make_directory()
        mock_load_results.return_value = pandas.DataFrame()

        results = self.matrix_converter._write_out_gene_dataframe(results_dir, 'genes.csv.gz', compression=True)

        self.assertEqual(type(results).__name__, 'DataFrame')
        mock_load_results.assert_called_once()
        mock_to_csv.assert_called_once_with('./test_target/genes.csv.gz',
                                            compression='gzip',
                                            index_label='featurekey',
                                            sep='\t')
        shutil.rmtree(results_dir)

    @mock.patch("pandas.DataFrame.to_csv")
    @mock.patch("matrix.common.query.feature_query_results_reader.FeatureQueryResultsReader.load_results")
    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test__write_out_gene_dataframe__without_compression(self, mock_parse_manifest, mock_load_results, mock_to_csv):
        self.matrix_converter.query_results = {
            QueryType.FEATURE: FeatureQueryResultsReader("test_manifest_key")
        }
        results_dir = self.matrix_converter._make_directory()
        mock_load_results.return_value = pandas.DataFrame()

        results = self.matrix_converter._write_out_gene_dataframe(results_dir, 'genes.csv', compression=False)

        self.assertEqual(type(results).__name__, 'DataFrame')
        mock_load_results.assert_called_once()
        mock_to_csv.assert_called_once_with('./test_target/genes.csv', index_label='featurekey')
        shutil.rmtree(results_dir)

    @mock.patch("pandas.DataFrame.reindex")
    @mock.patch("pandas.DataFrame.to_csv")
    def test__write_out_cell_dataframe__with_compression(self, mock_to_csv, mock_reindex):
        mock_reindex.return_value = pandas.DataFrame()
        results_dir = './test_target'
        results = self.matrix_converter._write_out_cell_dataframe(results_dir,
                                                                  'cells.csv.gz',
                                                                  pandas.DataFrame(),
                                                                  [],
                                                                  compression=True)

        self.assertEqual(type(results).__name__, 'DataFrame')
        mock_reindex.assert_called_once()
        mock_to_csv.assert_called_once_with('./test_target/cells.csv.gz',
                                            compression='gzip',
                                            index_label='cellkey',
                                            sep='\t')

    @mock.patch("pandas.DataFrame.reindex")
    @mock.patch("pandas.DataFrame.to_csv")
    def test__write_out_cell_dataframe__without_compression(self, mock_to_csv, mock_reindex):
        mock_reindex.return_value = pandas.DataFrame()
        results_dir = './test_target'
        results = self.matrix_converter._write_out_cell_dataframe(results_dir,
                                                                  'cells.csv',
                                                                  pandas.DataFrame(),
                                                                  [],
                                                                  compression=False)

        self.assertEqual(type(results).__name__, 'DataFrame')
        mock_reindex.assert_called_once()
        mock_to_csv.assert_called_once_with('./test_target/cells.csv', index_label='cellkey')

    def test_converter_with_file_formats(self):
        for file_format in SUPPORTED_FORMATS:
            with self.subTest(f"Converting to {file_format}"):
                self._test_converter_with_file_format(file_format)

    @mock.patch("os.remove")
    @mock.patch("os.mkdir")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.creation_date", new_callable=mock.PropertyMock)
    @mock.patch("scipy.sparse.hstack")
    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_request")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("s3fs.S3FileSystem.put")
    @mock.patch("scipy.io.mmwrite")
    @mock.patch("zipfile.ZipFile.write")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._to_csv")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._to_loom")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._to_mtx")
    @mock.patch("s3fs.S3Map.__init__")
    @mock.patch("s3fs.S3FileSystem.__init__")
    def _test_converter_with_file_format(self,
                                         file_format,
                                         mock_s3_fs,
                                         mock_s3_map,
                                         mock_to_mtx,
                                         mock_to_loom,
                                         mock_to_csv,
                                         mock_zipfile_write,
                                         mock_mmwrite,
                                         mock_s3_put,
                                         mock_complete_subtask_execution,
                                         mock_complete_request,
                                         mock_parse_manifest,
                                         mock_hstack,
                                         mock_creation_date,
                                         mock_os_mkdir,
                                         mock_os_remove):
        mock_s3_fs.return_value = None
        mock_s3_map.return_value = None
        mock_creation_date.return_value = date.to_string(datetime.datetime.utcnow())

        main(["test_id", "test_exp_manifest", "test_cell_manifest",
              "test_gene_manifest", "test_target", file_format, "."])

        if file_format == "loom":
            mock_to_loom.assert_called_once()
        elif file_format == "csv":
            mock_to_csv.assert_called_once()
        elif file_format == "mtx":
            mock_to_mtx.assert_called_once()

        mock_s3_put.assert_called_once()
        mock_complete_subtask_execution.assert_called_once_with(Subtask.CONVERTER)
        mock_complete_request.assert_called_once_with(duration=mock.ANY)

    def _test_unsupported_format(self):
        with self.assertRaises(SystemExit):
            main(["test_hash", "test_source_path", "target_path", "bad_format"])

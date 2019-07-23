import argparse
import datetime
import os
import shutil
import unittest
from unittest import mock

import pandas

from matrix.common import date
from matrix.common.request.request_tracker import Subtask
from matrix.docker.matrix_converter import main, MatrixConverter, SUPPORTED_FORMATS


class TestMatrixConverter(unittest.TestCase):

    def setUp(self):
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
        args = parser.parse_args(args)
        self.matrix_converter = MatrixConverter(args)

    @mock.patch("os.remove")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.creation_date", new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_request")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._upload_converted_matrix")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._to_loom")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._parse_manifest")
    def test_run(self,
                 mock_parse_manifest,
                 mock_to_loom,
                 mock_upload_converted_matrix,
                 mock_subtask_exec,
                 mock_complete_request,
                 mock_creation_date,
                 mock_os_remove):
        mock_creation_date.return_value = date.to_string(datetime.datetime.utcnow())
        mock_to_loom.return_value = "local_matrix_path"

        self.matrix_converter.run()

        mock_manifest_calls = [
            mock.call("test_exp_manifest"),
            mock.call("test_cell_manifest"),
            mock.call("test_gene_manifest")
        ]
        mock_parse_manifest.assert_has_calls(mock_manifest_calls)
        mock_to_loom.assert_called_once()
        mock_subtask_exec.assert_called_once_with(Subtask.CONVERTER)
        mock_complete_request.assert_called_once
        mock_upload_converted_matrix.assert_called_once_with("local_matrix_path", "test_target")

    @mock.patch("s3fs.S3FileSystem.open")
    def test__parse_manifest(self, mock_open):
        manifest_file_path = "tests/functional/res/cell_metadata_manifest"

        with open(manifest_file_path) as f:
            mock_open.return_value = f
            manifest = self.matrix_converter._parse_manifest("test_manifest_key")

        self.assertEqual(len(manifest['columns']), 23)
        self.assertEqual(manifest['record_count'], 2544)
        self.assertEqual(len(manifest['part_urls']), 8)
        self.assertTrue(all(u.startswith("s3://") for u in manifest['part_urls']))

    @mock.patch("s3fs.S3FileSystem.open")
    def test__n_slices(self, mock_open):
        manifest_file_path = "tests/functional/res/cell_metadata_manifest"
        with open(manifest_file_path) as f:
            mock_open.return_value = f
            self.matrix_converter.cell_manifest = self.matrix_converter._parse_manifest("test_manifest_key")

        self.assertEqual(self.matrix_converter._n_slices(), 8)

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
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._load_gene_table")
    def test__write_out_gene_dataframe__with_compression(self, mock_load_gene_table, mock_to_csv):
        results_dir = self.matrix_converter._make_directory()
        mock_load_gene_table.return_value = pandas.DataFrame()

        results = self.matrix_converter._write_out_gene_dataframe(results_dir, 'genes.csv.gz', compression=True)

        self.assertEqual(type(results).__name__, 'DataFrame')
        mock_load_gene_table.assert_called_once()
        mock_to_csv.assert_called_once_with('./test_target/genes.csv.gz',
                                            compression='gzip',
                                            index_label='featurekey',
                                            sep='\t')
        shutil.rmtree(results_dir)

    @mock.patch("pandas.DataFrame.to_csv")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._load_gene_table")
    def test__write_out_gene_dataframe__without_compression(self, mock_load_gene_table, mock_to_csv):
        results_dir = self.matrix_converter._make_directory()
        mock_load_gene_table.return_value = pandas.DataFrame()

        results = self.matrix_converter._write_out_gene_dataframe(results_dir, 'genes.csv', compression=False)

        self.assertEqual(type(results).__name__, 'DataFrame')
        mock_load_gene_table.assert_called_once()
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

    @mock.patch("pandas.read_csv")
    @mock.patch("s3fs.S3FileSystem.open")
    def test__load_cell_table_slice(self, mock_open, mock_pd_read_csv):
        manifest_file_path = "tests/functional/res/cell_metadata_manifest"
        with open(manifest_file_path) as f:
            mock_open.return_value = f
            self.matrix_converter.cell_manifest = self.matrix_converter._parse_manifest("test_manifest_key")

        self.matrix_converter._load_cell_table_slice(3)

        pandas_args = mock_pd_read_csv.call_args[-2]
        pandas_kwargs = mock_pd_read_csv.call_args[-1]

        self.assertIn("project.project_core.project_short_name", pandas_kwargs["names"])
        self.assertTrue(pandas_args[0].startswith("s3://"))

    @mock.patch("s3fs.S3FileSystem.open")
    def test__load_expression_table_slice(self, mock_open):
        manifest_file_path = "tests/functional/res/expression_metadata_manifest"
        with open(manifest_file_path) as f:
            mock_open.return_value = f
            self.matrix_converter.expression_manifest = self.matrix_converter._parse_manifest("test_manifest_key")
        results = self.matrix_converter._load_expression_table_slice(0)
        mock_open.assert_called_once()
        self.assertEqual(type(results).__name__, 'generator')

    @mock.patch("pandas.read_csv")
    @mock.patch("s3fs.S3FileSystem.open")
    def test__load_gene_table(self, mock_open, mock_pd_read_csv):
        manifest_file_path = "tests/functional/res/gene_metadata_manifest"
        with open(manifest_file_path) as f:
            mock_open.return_value = f
            mock_pd_read_csv.return_value = pandas.DataFrame()
            self.matrix_converter.gene_manifest = self.matrix_converter._parse_manifest("test_manifest_key")
            self.matrix_converter._load_gene_table()
            mock_open.assert_called_once()
            mock_pd_read_csv.assert_called_once()

    def test_converter_with_file_formats(self):
        for file_format in SUPPORTED_FORMATS:
            with self.subTest(f"Converting to {file_format}"):
                self._test_converter_with_file_format(file_format)

    @mock.patch("os.remove")
    @mock.patch("os.mkdir")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.creation_date", new_callable=mock.PropertyMock)
    @mock.patch("scipy.sparse.hstack")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._parse_manifest")
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

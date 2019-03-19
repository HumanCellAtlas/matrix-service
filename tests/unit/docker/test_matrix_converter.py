import argparse
import datetime
import unittest
from unittest import mock

import pandas

from matrix.common import date
from matrix.common.request.request_tracker import Subtask
from matrix.docker.matrix_converter import main, MatrixConverter, SUPPORTED_FORMATS


class TestMatrixConverter(unittest.TestCase):

    def setUp(self):
        args = ["test_id", "test_exp_manifest", "test_cell_manifest", "test_gene_manifest", "test_target", "loom"]
        parser = argparse.ArgumentParser()
        parser.add_argument("request_id")
        parser.add_argument("expression_manifest_key")
        parser.add_argument("cell_metadata_manifest_key")
        parser.add_argument("gene_metadata_manifest_key")
        parser.add_argument("target_path")
        parser.add_argument("format", choices=SUPPORTED_FORMATS)
        args = parser.parse_args(args)
        self.matrix_converter = MatrixConverter(args)

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
                 mock_creation_date):
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
        manifest_file_path = "tests/functional/res/manifest.json"

        with open(manifest_file_path) as f:
            mock_open.return_value = f
            manifest = self.matrix_converter._parse_manifest("test_manifest_key")

        self.assertEqual(manifest['columns'], ['cellkey', 'featurekey', 'exrpvalue'])
        self.assertEqual(manifest['record_count'], 0)
        self.assertEqual(manifest['part_urls'], [])

    def test_converter_with_file_formats(self):
        for file_format in SUPPORTED_FORMATS:
            with self.subTest(f"Converting to {file_format}"):
                self._test_converter_with_file_format(file_format)

    @mock.patch("os.mkdir")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.creation_date", new_callable=mock.PropertyMock)
    @mock.patch("scipy.sparse.hstack")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._parse_manifest")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._load_table")
    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._load_table_by_part")
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
                                         mock_load_table_by_part,
                                         mock_load_table,
                                         mock_parse_manifest,
                                         mock_hstack,
                                         mock_creation_date,
                                         mock_os_mkdir):
        mock_s3_fs.return_value = None
        mock_s3_map.return_value = None
        mock_creation_date.return_value = date.to_string(datetime.datetime.utcnow())
        mock_load_table.return_value = pandas.DataFrame()

        main(["test_id", "test_exp_manifest", "test_cell_manifest", "test_gene_manifest", "test_target", file_format])

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

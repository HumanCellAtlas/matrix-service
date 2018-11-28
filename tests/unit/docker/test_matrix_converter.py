import os
import unittest
from unittest import mock

import zarr

from matrix.common.request.request_tracker import Subtask
from matrix.docker.matrix_converter import main, SUPPORTED_FORMATS

PATH_TO_ZARR = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            "..",
                            "..",
                            "functional",
                            "res",
                            "test_zarr.zip")


class TestMatrixConverter(unittest.TestCase):

    def setUp(self):
        self.group = zarr.group(store=PATH_TO_ZARR)

    def test_converter_with_file_formats(self):
        for file_format in SUPPORTED_FORMATS:
            with self.subTest(f"Converting to {file_format}"):
                self._test_converter_with_file_format(file_format)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("s3fs.S3FileSystem.put")
    @mock.patch("scipy.io.mmwrite")
    @mock.patch("zipfile.ZipFile.write")
    @mock.patch("pandas.DataFrame.to_csv")
    @mock.patch("loompy.create")
    @mock.patch("zarr.group")
    @mock.patch("s3fs.S3Map.__init__")
    @mock.patch("s3fs.S3FileSystem.__init__")
    def _test_converter_with_file_format(self,
                                         file_format,
                                         mock_s3_fs,
                                         mock_s3_map,
                                         mock_group,
                                         mock_loompy_create,
                                         mock_to_csv,
                                         mock_zipfile_write,
                                         mock_mmwrite,
                                         mock_s3_put,
                                         mock_complete_subtask_execution):
        mock_s3_fs.return_value = None
        mock_s3_map.return_value = None
        mock_group.return_value = self.group

        main(["test_hash", "test_source_path", "test_target_path", file_format])

        if file_format == "loom":
            mock_loompy_create.assert_called_once()
        elif file_format == "csv":
            mock_to_csv.assert_called_once()
        elif file_format == "mtx":
            mock_mmwrite.assert_called_once()
            self.assertEqual(mock_zipfile_write.call_count, 2)

        mock_s3_put.assert_called_once()
        mock_complete_subtask_execution.assert_called_once_with(Subtask.CONVERTER)

    def test_unsupported_format(self):
        with self.assertRaises(SystemExit):
            main(["test_hash", "test_source_path", "target_path", "bad_format"])

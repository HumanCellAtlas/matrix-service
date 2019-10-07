import mock
import unittest

import pandas

from matrix.common.query.cell_query_results_reader import CellQueryResultsReader


class TestCellQueryResultsReader(unittest.TestCase):
    @mock.patch("matrix.common.query.cell_query_results_reader.CellQueryResultsReader.load_slice")
    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_load_results(self, mock_parse_manifest, mock_load_slice):
        mock_parse_manifest.return_value = {
            "columns": ["a", "b", "c"],
            "part_urls": ["A", "B", "C"],
            "record_count": 5
        }
        test_df = pandas.DataFrame()
        mock_load_slice.return_value = test_df
        reader = CellQueryResultsReader("test_manifest_key")
        reader.load_results()

        expected_calls = [mock.call(0), mock.call(1), mock.call(2)]
        mock_load_slice.assert_has_calls(expected_calls)

    @mock.patch("pandas.read_csv")
    @mock.patch("s3fs.S3FileSystem.open")
    def test_load_slice(self, mock_open, mock_pd_read_csv):
        manifest_file_path = "tests/functional/res/cell_metadata_manifest"
        with open(manifest_file_path) as f:
            mock_open.return_value = f
            reader = CellQueryResultsReader("test_manifest_key")

            reader.load_slice(3)

        pandas_args = mock_pd_read_csv.call_args[-2]
        pandas_kwargs = mock_pd_read_csv.call_args[-1]

        self.assertIn("project.project_core.project_short_name", pandas_kwargs["names"])
        self.assertTrue(pandas_args[0].startswith("s3://"))

    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_load_empty_results(self, mock_parse_manifest):

        mock_parse_manifest.return_value = {"record_count": 0}
        cell_query_results_reader = CellQueryResultsReader("test_manifest_key")

        results = cell_query_results_reader.load_results()
        self.assertEqual(results.shape, (0, 0))

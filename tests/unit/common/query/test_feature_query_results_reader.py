import mock
import unittest

import pandas

from matrix.common.query.feature_query_results_reader import FeatureQueryResultsReader


class TestFeatureQueryResultsReader(unittest.TestCase):
    @mock.patch("pandas.read_csv")
    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_load_results(self, mock_parse_manifest, mock_read_csv):
        mock_parse_manifest.return_value = {
            "columns": ["a", "b", "c"],
            "part_urls": ["A", "B", "C"],
            "record_count": 5
        }
        mock_read_csv.return_value = pandas.DataFrame()
        reader = FeatureQueryResultsReader("test_manifest_key")
        reader.load_results()
        self.assertEqual(mock_read_csv.call_count, 3)

    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_load_slice(self, mock_parse_manifest):
        reader = FeatureQueryResultsReader("test_manifest_key")
        with self.assertRaises(NotImplementedError):
            reader.load_slice(0)

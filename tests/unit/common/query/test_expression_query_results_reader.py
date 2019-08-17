import mock
import unittest

from matrix.common.query.expression_query_results_reader import ExpressionQueryResultsReader


class TestExpressionQueryResultsReader(unittest.TestCase):
    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_load_results(self, mock_parse_manifest):
        reader = ExpressionQueryResultsReader("test_manifest_key")
        with self.assertRaises(NotImplementedError):
            reader.load_results()

    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_load_slice(self, mock_parse_manifest):
        reader = ExpressionQueryResultsReader("test_manifest_key")
        results = reader.load_slice(0)
        self.assertEqual(type(results).__name__, 'generator')

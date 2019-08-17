import mock
import unittest

from matrix.common.query.query_results_reader import QueryResultsReader


class TestQueryResultsReader(unittest.TestCase):

    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    @mock.patch("s3fs.S3FileSystem.open")
    def test_init(self, mock_open, mock_parse_manifest):
        manifest_file_path = "tests/functional/res/cell_metadata_manifest"

        with open(manifest_file_path) as f:
            mock_open.return_value = f
            QueryResultsReader("test_manifest_key")
            mock_parse_manifest.assert_called_once_with("test_manifest_key")

    @mock.patch("s3fs.S3FileSystem.open")
    def test_parse_manifest(self, mock_open):
        manifest_file_path = "tests/functional/res/cell_metadata_manifest"

        with open(manifest_file_path) as f:
            mock_open.return_value = f
            query_results_reader = QueryResultsReader("test_manifest_key")

        self.assertEqual(len(query_results_reader.manifest['columns']), 23)
        self.assertEqual(query_results_reader.manifest['record_count'], 2544)
        self.assertEqual(len(query_results_reader.manifest['part_urls']), 8)
        self.assertTrue(all(u.startswith("s3://") for u in query_results_reader.manifest['part_urls']))

    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_load_results(self, mock_parse_manifest):
        query_results_reader = QueryResultsReader("test_manifest_key")
        with self.assertRaises(NotImplementedError):
            query_results_reader.load_results()

    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_load_slice(self, mock_parse_manifest):
        query_results_reader = QueryResultsReader("test_manifest_key")
        with self.assertRaises(NotImplementedError):
            query_results_reader.load_slice("test_index")

    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_map_columns(self, mock_parse_manifest):
        query_results_reader = QueryResultsReader("test_manifest_key")
        test_table_columns = ["test_column", "bundle_fqid"]
        metadata_fields = query_results_reader._map_columns(test_table_columns)
        self.assertEqual(metadata_fields, ["test_column", "dss_bundle_fqid"])

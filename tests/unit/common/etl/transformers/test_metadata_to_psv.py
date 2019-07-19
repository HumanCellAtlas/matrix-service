import mock
import unittest

from matrix.common.etl.transformers import MetadataToPsvTransformer
from matrix.common.aws.redshift_handler import TableName


class TestMetadataToPsvTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = MetadataToPsvTransformer("")
        self.test_table_data = [
            (TableName.CELL, ["cell_row_1", "cell_row_2"]),
            (TableName.EXPRESSION, ["expr_row_1", "expr_row_2"])
        ]

    @mock.patch("matrix.common.etl.transformers.MetadataToPsvTransformer._write_rows_to_psvs")
    @mock.patch("matrix.common.etl.transformers.MetadataToPsvTransformer._parse_from_metadatas")
    def test_transform(self, mock_parse_from_metadatas, mock_write_rows_to_psvs):
        test_bundle_dir = "test_bundle_dir"
        mock_parse_from_metadatas.return_value = (self.test_table_data[0],
                                                  self.test_table_data[1])

        self.transformer.transform(test_bundle_dir)

        mock_parse_from_metadatas.assert_called_once_with(test_bundle_dir)
        mock_write_rows_to_psvs.assert_called_once_with(self.test_table_data[0],
                                                        self.test_table_data[1])

    def test_write_rows_to_psvs(self):
        with mock.patch("builtins.open", mock.mock_open()) as mock_open:
            self.transformer._write_rows_to_psvs(self.test_table_data[0],
                                                 self.test_table_data[1])
            handle = mock_open()
            expected_calls = [
                mock.call("cell_row_1\n"),
                mock.call("cell_row_2\n"),
                mock.call("expr_row_1\n"),
                mock.call("expr_row_2\n")
            ]
            handle.write.assert_has_calls(expected_calls)

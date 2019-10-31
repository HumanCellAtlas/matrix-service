import mock
import os
import unittest

from matrix.common.aws.redshift_handler import TableName
from matrix.common.etl.transformers.cell_expression import CellExpressionTransformer


class TestCellExpressionTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = CellExpressionTransformer("")
        self.test_table_data = [
            (TableName.CELL, ["cell_row_1", "cell_row_2"], "path/to/bundle"),
            (TableName.EXPRESSION, ["expr_row_1", "expr_row_2"], "path/to/bundle")
        ]

    def test_write_rows_to_psvs(self):
        with mock.patch("gzip.open", mock.mock_open()) as mock_open:
            self.transformer._write_rows_to_psvs(self.test_table_data[0],
                                                 self.test_table_data[1])
            handle = mock_open()
            mock_open.assert_any_call("output/cell/bundle.cell.data.gz", "w")
            mock_open.assert_any_call("output/expression/bundle.expression.data.gz", "w")
            self.assertEqual(handle.writelines.call_count, 2)

    def test_parse_ss2(self):
        parsed = self.transformer._parse_from_metadatas(
            "tests/functional/res/etl/ss2_bundle.version",
            "tests/functional/res/etl/ss2_bundle_manifest.json")

        cell_table = parsed[0][0]
        cell_rows = parsed[0][1]
        self.assertEqual(cell_table, TableName.CELL)
        self.assertEqual(cell_rows[0],
                         "635badd5-7d62-4db3-b509-f290a12a1336|635badd5-7d62-4db3-b509-f290a12a1336|"
                         "c3ba122b-9158-4447-b379-6f5983a2416d|"
                         "265ab074-6db1-4038-836c-fba3cc2d09cb|f6ff0075-f93e-478a-8ba3-8c798e7f5021|"
                         "436cd3a5-e510-41db-937d-6c5f4f1b6df7|2019-01-28T133934.450115Z||3859||\n")

        expression_table = parsed[1][0]
        expression_rows = parsed[1][1]
        self.assertEqual(expression_table, TableName.EXPRESSION)
        self.assertEqual(expression_rows[0], "635badd5-7d62-4db3-b509-f290a12a1336|ENST00000373020|TPM|92.29\n")

    def test_parse_optimus(self):
        parsed = self.transformer._parse_from_metadatas(
            bundle_dir=os.path.abspath("tests/functional/res/etl/optimus_bundle.version"),
            bundle_manifest_path=os.path.abspath("tests/functional/res/etl/optimus_bundle_manifest.json")
        )
        cell_lines = parsed[0][1]
        expression_lines = parsed[1][1]

        self.assertEqual(len(cell_lines), 5)
        self.assertTrue("5469c35c54d5b403cb00da7d9ea16879|493a6adc-54b5-4388-ba11-c37686562127|"
                        "dbb40797-8eba-44f8-81d8-6f0c2e2ed0b5|"
                        "ffb71426-42a4-42c0-89cc-f12b4a806554|17987139-5441-4335-8a36-2ec986eee282|"
                        "ae725a64-6cb4-4216-942f-37880ed52ed3|2019-05-08T155712.599791Z|"
                        "AGTGGGAGTACAGACG|12|142|f\n" in cell_lines)

        self.assertEqual(len(expression_lines), 174)
        self.assertEqual(expression_lines[0], "5469c35c54d5b403cb00da7d9ea16879|GUK1|Count|1.0\n")

import os
import unittest

from matrix.common.aws.redshift_handler import TableName
from matrix.common.etl.transformers.cell_expression import CellExpressionTransformer


class TestCellExpressionTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = CellExpressionTransformer("")

    def test_parse_from_metadatas(self):
        parsed = self.transformer._parse_from_metadatas("tests/functional/res/etl/bundle")

        cell_table = parsed[0][0]
        cell_rows = parsed[0][1]
        self.assertEqual(cell_table, TableName.CELL)
        self.assertEqual(cell_rows[0],
                         "635badd5-7d62-4db3-b509-f290a12a1336|635badd5-7d62-4db3-b509-f290a12a1336|"
                         "c3ba122b-9158-4447-b379-6f5983a2416d|80bd863b-d92c-4c5f-98b6-4c32d7b2e806|"
                         "265ab074-6db1-4038-836c-fba3cc2d09cb|f6ff0075-f93e-478a-8ba3-8c798e7f5021||3859\n")

        expression_table = parsed[1][0]
        expression_rows = parsed[1][1]
        self.assertEqual(expression_table, TableName.EXPRESSION)
        self.assertEqual(expression_rows[0], "635badd5-7d62-4db3-b509-f290a12a1336|ENST00000373020|TPM|92.29\n")

    def test_parse_optimus(self):
        cell_lines, expression_lines = self.transformer._parse_optimus_bundle(
            bundle_dir=os.path.abspath("tests/functional/res/etl/d65ef9e6-3fd1-4026-9843-ff573c9e66c8")
        )
        self.assertEqual(len(cell_lines), 5)
        self.assertTrue("aaa573518b66eb105c09fd34d0418a13|493a6adc-54b5-4388-ba11-c37686562127|"
                        "dbb40797-8eba-44f8-81d8-6f0c2e2ed0b5|88bc1e69-624e-4e12-b0a2-e1b64832ec3f|"
                        "ffb71426-42a4-42c0-89cc-f12b4a806554|17987139-5441-4335-8a36-2ec986eee282|"
                        "GTCGGGTTCACGGTTA|71\n" in cell_lines)

        self.assertEqual(len(expression_lines), 174)
        self.assertEqual(expression_lines[0], "0ef1dea5d73efc2f4cfaaeeaea2dd10c|GUK1|Count|1.0\n")

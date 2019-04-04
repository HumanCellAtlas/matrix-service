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

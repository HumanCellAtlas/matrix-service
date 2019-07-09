import unittest

from matrix.common.aws.redshift_handler import TableName
from matrix.common.etl.transformers.analysis import AnalysisTransformer


class TestAnalysisTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = AnalysisTransformer("")

    def test_parse_from_metadatas(self):
        parsed = self.transformer._parse_from_metadatas("tests/functional/res/etl/ss2_bundle")

        analysis_table = parsed[0][0]
        analysis_rows = parsed[0][1]
        self.assertEqual(analysis_table, TableName.ANALYSIS)
        self.assertTrue("f6ff0075-f93e-478a-8ba3-8c798e7f5021|ss2_bundle|smartseq2_v2.2.0|blessed" in analysis_rows)

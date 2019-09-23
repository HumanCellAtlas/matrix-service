import unittest
from unittest import mock

from matrix.common.aws.redshift_handler import TableName
from matrix.common.etl.transformers.feature import FeatureTransformer


class TestFeatureTransformer(unittest.TestCase):

    @mock.patch("matrix.common.etl.transformers.feature.FeatureTransformer._fetch_annotations")
    def test_parse_from_metadatas(self, mock_fetch_annotations):
        transformer = FeatureTransformer("")
        transformer.annotation_files = {
            "Homo sapiens": "tests/functional/res/etl/annotation.gtf"
        }
        parsed = transformer._parse_from_metadatas("")

        feature_table = parsed[0][0]
        feature_rows = parsed[0][1]
        self.assertEqual(feature_table, TableName.FEATURE)
        self.assertTrue("ENST00000619216|MIR6859-1-201|miRNA|chr1|17369|17436|False|Homo sapiens" in feature_rows)

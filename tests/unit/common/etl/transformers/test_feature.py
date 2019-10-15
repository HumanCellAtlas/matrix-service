import gzip
import os
import shutil
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

    @mock.patch("urllib.request.urlretrieve")
    def test_fetch_annotations(self, mock_urlretrieve):
        os.makedirs(os.path.join("test_fetch_annotations", "Homo sapiens"))
        os.makedirs(os.path.join("test_fetch_annotations", "Mus musculus"))

        with gzip.open(os.path.join("test_fetch_annotations", "Homo sapiens",
                                    "gencode_annotation.gtf.gz"), 'wb') as gtf_gz:
            gtf_gz.write(b"genes! genes! genes!")

        with gzip.open(os.path.join("test_fetch_annotations", "Mus musculus",
                                    "gencode_annotation.gtf.gz"), 'wb') as gtf_gz:
            gtf_gz.write(b"genes! genes! genes!")

        transformer = FeatureTransformer("test_fetch_annotations")

        self.assertDictEqual(
            {"Homo sapiens": os.path.join("test_fetch_annotations", "Homo sapiens", "gencode_annotation.gtf"),
             "Mus musculus": os.path.join("test_fetch_annotations", "Mus musculus", "gencode_annotation.gtf")},
            transformer.annotation_files)

        shutil.rmtree("test_fetch_annotations")

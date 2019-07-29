import unittest
from unittest import mock

from matrix.common.aws.redshift_handler import TableName
from matrix.common.etl.transformers.specimen_library import SpecimenLibraryTransformer


class TestSpecimenLibraryTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = SpecimenLibraryTransformer("")

    @mock.patch("requests.get")
    def test_parse_from_metadatas(self, mock_get):
        resp = mock.Mock()
        resp.json.return_value = {'label': 'EXAMPLE_ONTOTLOGY'}
        mock_get.return_value = resp
        parsed = self.transformer._parse_from_metadatas("tests/functional/res/etl/ss2_bundle.version")

        specimen_table = parsed[0][0]
        specimen_rows = parsed[0][1]
        self.assertEqual(specimen_table, TableName.SPECIMEN)
        self.assertTrue("80bd863b-d92c-4c5f-98b6-4c32d7b2e806|NCBITAXON:9606|EXAMPLE_ONTOTLOGY|HANCESTRO:0016|"
                        "EXAMPLE_ONTOTLOGY|MONDO:0011273|EXAMPLE_ONTOTLOGY|EFO:0001272|EXAMPLE_ONTOTLOGY|"
                        "UBERON:0002113|EXAMPLE_ONTOTLOGY|UBERON:0014451|EXAMPLE_ONTOTLOGY" in specimen_rows)

        library_table = parsed[1][0]
        library_rows = parsed[1][1]
        self.assertEqual(library_table, TableName.LIBRARY_PREPARATION)
        self.assertTrue("265ab074-6db1-4038-836c-fba3cc2d09cb|OBI:0000869|EXAMPLE_ONTOTLOGY|"
                        "EFO:0008931|EXAMPLE_ONTOTLOGY|full length|unstranded" in library_rows)

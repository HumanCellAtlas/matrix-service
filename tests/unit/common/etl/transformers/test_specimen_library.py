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
        resp.json.return_value = {'label': 'EXAMPLE_ONTOLOGY'}
        mock_get.return_value = resp
        parsed = self.transformer._parse_from_metadatas("tests/functional/res/etl/ss2_bundle.version")

        specimen_table = parsed[0][0]
        specimen_rows = parsed[0][1]
        self.assertEqual(specimen_table, TableName.SPECIMEN)
        self.assertTrue("80bd863b-d92c-4c5f-98b6-4c32d7b2e806|UBERON:0002113|EXAMPLE_ONTOLOGY|"
                        "UBERON:0014451|EXAMPLE_ONTOLOGY|MONDO:0011273|EXAMPLE_ONTOLOGY" in specimen_rows)

        library_table = parsed[1][0]
        library_rows = parsed[1][1]
        self.assertEqual(library_table, TableName.LIBRARY_PREPARATION)
        self.assertTrue("265ab074-6db1-4038-836c-fba3cc2d09cb|OBI:0000869|EXAMPLE_ONTOLOGY|"
                        "EFO:0008931|EXAMPLE_ONTOLOGY|full length|unstranded" in library_rows)

        donor_table = parsed[2][0]
        donor_rows = parsed[2][1]
        self.assertEqual(donor_table, TableName.DONOR)
        self.assertTrue("138c5ad0-17e9-4ab1-b1ca-41b19455f859|HANCESTRO:0016|EXAMPLE_ONTOLOGY|"
                        "MONDO:0011273|EXAMPLE_ONTOLOGY|EFO:0001272|EXAMPLE_ONTOLOGY|unknown|unknown"
                        in donor_rows)

        cs_table = parsed[3][0]
        cs_rows = parsed[3][1]
        self.assertEqual(cs_table, TableName.CELL_SUSPENSION)
        self.assertTrue("635badd5-7d62-4db3-b509-f290a12a1336|80bd863b-d92c-4c5f-98b6-4c32d7b2e806|"
                        "138c5ad0-17e9-4ab1-b1ca-41b19455f859|UBERON:0002113|EXAMPLE_ONTOLOGY|"
                        "UBERON:0014451|EXAMPLE_ONTOLOGY|NCBITAXON:9606|EXAMPLE_ONTOLOGY"
                        in cs_rows)

    @mock.patch("requests.get")
    def test_multiple_donors(self, mock_get):
        resp = mock.Mock()
        resp.json.return_value = {'label': 'EXAMPLE_ONTOLOGY'}
        mock_get.return_value = resp
        parsed = self.transformer._parse_from_metadatas("tests/functional/res/etl/organoid_bundle.version")

        specimen_table = parsed[0][0]
        specimen_rows = parsed[0][1]
        self.assertEqual(specimen_table, TableName.SPECIMEN)
        self.assertTrue("b952a25e-6a70-4956-abf3-c195bae4c1b6|UBERON:0002097|EXAMPLE_ONTOLOGY|"
                        "UBERON:0001003|EXAMPLE_ONTOLOGY|PATO:0000461|EXAMPLE_ONTOLOGY" in specimen_rows)

        library_table = parsed[1][0]
        library_rows = parsed[1][1]
        self.assertEqual(library_table, TableName.LIBRARY_PREPARATION)
        self.assertTrue("910266c3-64b1-4a3d-a4fe-844be494ffd1|OBI:0000869|EXAMPLE_ONTOLOGY|"
                        "EFO:0009310|EXAMPLE_ONTOLOGY|3 prime tag|first" in library_rows)

        donor_table = parsed[2][0]
        donor_rows = parsed[2][1]
        self.assertEqual(donor_table, TableName.DONOR)
        self.assertTrue("1399aa16-3fb0-4a04-a8d2-3af7e079ebf3|||"
                        "PATO:0000461|EXAMPLE_ONTOLOGY|HSAPDV:0000087|EXAMPLE_ONTOLOGY||yes"
                        in donor_rows)

        cs_table = parsed[3][0]
        cs_rows = parsed[3][1]
        self.assertEqual(cs_table, TableName.CELL_SUSPENSION)
        self.assertTrue("a286da91-6e9f-42dd-ae7b-9ee37fba9529|b952a25e-6a70-4956-abf3-c195bae4c1b6|"
                        "1399aa16-3fb0-4a04-a8d2-3af7e079ebf3|UBERON:0000955 (organoid)|EXAMPLE_ONTOLOGY (organoid)|"
                        "||NCBITAXON:9606|EXAMPLE_ONTOLOGY"
                        in cs_rows)

    @mock.patch("requests.get")
    def test_cell_line(self, mock_get):
        resp = mock.Mock()
        resp.json.return_value = {'label': 'EXAMPLE_ONTOLOGY'}
        mock_get.return_value = resp
        parsed = self.transformer._parse_from_metadatas("tests/functional/res/etl/cell_line_bundle.version")

        specimen_table = parsed[0][0]
        specimen_rows = parsed[0][1]
        self.assertEqual(specimen_table, TableName.SPECIMEN)
        self.assertTrue("20156407-a6f8-447e-a21e-dbbf66a58043|UBERON:0002390|EXAMPLE_ONTOLOGY|"
                        "UBERON:0002371|EXAMPLE_ONTOLOGY|PATO:0000461|EXAMPLE_ONTOLOGY" in specimen_rows)

        library_table = parsed[1][0]
        library_rows = parsed[1][1]
        self.assertEqual(library_table, TableName.LIBRARY_PREPARATION)
        self.assertTrue("dc19bb22-ae7b-431b-9b8b-7b49799a8fcd|CHEBI:33699|EXAMPLE_ONTOLOGY|"
                        "EFO:0009310|EXAMPLE_ONTOLOGY|3 prime tag|first" in library_rows)

        donor_table = parsed[2][0]
        donor_rows = parsed[2][1]
        self.assertEqual(donor_table, TableName.DONOR)
        self.assertTrue("e24f10c2-e21d-44c4-9450-46029c443941|HANCESTRO:0005|EXAMPLE_ONTOLOGY|"
                        "PATO:0000461|EXAMPLE_ONTOLOGY|HSAPDV:0000087|EXAMPLE_ONTOLOGY|female|yes"
                        in donor_rows)

        cs_table = parsed[3][0]
        cs_rows = parsed[3][1]
        self.assertEqual(cs_table, TableName.CELL_SUSPENSION)
        self.assertTrue("907ccd95-51af-4a95-b641-9f364730c127|20156407-a6f8-447e-a21e-dbbf66a58043|"
                        "e24f10c2-e21d-44c4-9450-46029c443941|UBERON:0002390 (cell line)|EXAMPLE_ONTOLOGY (cell line)|"
                        "||NCBITAXON:9606|EXAMPLE_ONTOLOGY"
                        in cs_rows)

    @mock.patch("requests.get")
    def test_multiple_ethnicity(self, mock_get):
        resp = mock.Mock()
        resp.json.return_value = {'label': 'EXAMPLE_ONTOLOGY'}
        mock_get.return_value = resp
        parsed = self.transformer._parse_from_metadatas("tests/functional/res/etl/multiple_ethnicity_bundle.version")

        donor_table = parsed[2][0]
        donor_rows = parsed[2][1]
        self.assertEqual(donor_table, TableName.DONOR)
        self.assertTrue("219098db-6a8d-4e5c-9cd9-8cfcac665a4c|HANCESTRO:0568;HANCESTRO:0014|"
                        "EXAMPLE_ONTOLOGY;EXAMPLE_ONTOLOGY|"
                        "PATO:0000461|EXAMPLE_ONTOLOGY|HSAPDV:0000087|EXAMPLE_ONTOLOGY|male|yes"
                        in donor_rows)

    @mock.patch("requests.get")
    def test_multiple_specimen_use(self, mock_get):
        resp = mock.Mock()
        resp.json.return_value = {'label': 'EXAMPLE_ONTOLOGY'}
        mock_get.return_value = resp
        parsed = self.transformer._parse_from_metadatas("tests/functional/res/etl/dendritic")

        specimen_table = parsed[0][0]
        specimen_rows = parsed[0][1]
        self.assertEqual(specimen_table, TableName.SPECIMEN)
        self.assertEqual(len(specimen_rows), 1)
        self.assertTrue("d6a518a8-0c5d-4cb0-aed5-68f3455c2bda|UBERON:0000922|EXAMPLE_ONTOLOGY|"
                        "UBERON:0001003|EXAMPLE_ONTOLOGY|PATO:0000461|EXAMPLE_ONTOLOGY" in specimen_rows)

        library_table = parsed[1][0]
        library_rows = parsed[1][1]
        self.assertEqual(library_table, TableName.LIBRARY_PREPARATION)
        self.assertEqual(len(library_rows), 1)
        self.assertTrue("2945bb1f-90de-42a3-afa1-f57a62c853f0|CHEBI:33699|EXAMPLE_ONTOLOGY|"
                        "EFO:0009310|EXAMPLE_ONTOLOGY|3 prime tag|second" in library_rows)

        donor_table = parsed[2][0]
        donor_rows = parsed[2][1]
        self.assertEqual(donor_table, TableName.DONOR)
        self.assertEqual(len(donor_rows), 1)
        self.assertTrue("2b7adb0a-82a4-4319-80d1-4a73d879dec1|||"
                        "PATO:0000461|EXAMPLE_ONTOLOGY|HSAPDV:0000025|EXAMPLE_ONTOLOGY|unknown|no"
                        in donor_rows)

        cs_table = parsed[3][0]
        cs_rows = parsed[3][1]
        self.assertEqual(cs_table, TableName.CELL_SUSPENSION)
        self.assertTrue("a3352e34-e7ae-4ed0-b580-a10c2f7a3451|d6a518a8-0c5d-4cb0-aed5-68f3455c2bda|"
                        "2b7adb0a-82a4-4319-80d1-4a73d879dec1|UBERON:0002405 (cell line)|EXAMPLE_ONTOLOGY (cell line)|"
                        "||NCBITAXON:9606|EXAMPLE_ONTOLOGY"
                        in cs_rows)
        self.assertTrue("0e44628a-552b-4377-a262-26ef6cdfe104|d6a518a8-0c5d-4cb0-aed5-68f3455c2bda|"
                        "2b7adb0a-82a4-4319-80d1-4a73d879dec1|UBERON:0000922|EXAMPLE_ONTOLOGY|"
                        "UBERON:0001003|EXAMPLE_ONTOLOGY|NCBITAXON:9606|EXAMPLE_ONTOLOGY"
                        in cs_rows)

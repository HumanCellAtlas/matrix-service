import unittest

from matrix.common.aws.redshift_handler import TableName
from matrix.common.etl.transformers.project_publication_contributor import ProjectPublicationContributorTransformer


class TestProjectPublicationContributor(unittest.TestCase):
    def setUp(self):
        self.transformer = ProjectPublicationContributorTransformer("")

    def test_parse_from_metadatas(self):
        parsed = self.transformer._parse_from_metadatas("tests/functional/res/etl/ss2_bundle.version")

        project_table = parsed[0][0]
        project_rows = parsed[0][1]
        self.assertEqual(project_table, TableName.PROJECT)
        self.assertTrue("c3ba122b-9158-4447-b379-6f5983a2416d|"
                        "scale/ss2_4000/2019-01-27T22:57:42Z|SS2 1 Cell Integration Test" in project_rows)

        contributor_table = parsed[1][0]
        contributor_rows = parsed[1][1]
        self.assertEqual(contributor_table, TableName.CONTRIBUTOR)
        self.assertTrue("c3ba122b-9158-4447-b379-6f5983a2416d|Jane,,Smith|University of Washington" in contributor_rows)

        publication_table = parsed[2][0]
        publication_rows = parsed[2][1]
        self.assertEqual(publication_table, TableName.PUBLICATION)
        self.assertTrue("c3ba122b-9158-4447-b379-6f5983a2416d|"
                        "Study of single cells in the human body|10.1016/j.cell.2016.07.054" in publication_rows)

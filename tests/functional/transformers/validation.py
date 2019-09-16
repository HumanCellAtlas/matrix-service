import unittest
import typing

from matrix.common.aws.redshift_handler import TableName
from matrix.common.constants import BundleType


class TransformerValidator(unittest.TestCase):
    def validate(self, actual_rows: typing.Tuple, expected_rows: dict, bundle_type: BundleType):
        raise NotImplementedError()


class AnalysisValidator(TransformerValidator):
    def validate(self, actual_rows: typing.Tuple, expected_rows: dict, bundle_type: BundleType):
        analysis_rows = actual_rows[0][1]

        # assumes 1 analysis row per bundle
        actual_vals = next(iter(analysis_rows)).split("|")
        expected_vals = expected_rows[TableName.ANALYSIS].split("|")

        # ignore bundle_fqid as version may change
        actual_vals.pop(1)
        expected_vals.pop(1)

        self.assertEqual(actual_vals, expected_vals)


class CellExpressionValidator(TransformerValidator):
    def validate(self, actual_rows: typing.Tuple, expected_rows: dict, bundle_type: BundleType):
        cell_rows = actual_rows[0][1]
        if bundle_type == BundleType.SS2:
            self.assertEqual(cell_rows[0], expected_rows[TableName.CELL])

        expression_rows = actual_rows[1][1]
        self.assertEqual(expression_rows[0], expected_rows[TableName.EXPRESSION])


class ProjectPublicationContributorValidator(TransformerValidator):
    def validate(self, actual_rows: typing.Tuple, expected_rows: dict, bundle_type: BundleType):
        project_rows = actual_rows[0][1]
        self.assertTrue(expected_rows[TableName.PROJECT] in project_rows)

        contributor_rows = actual_rows[1][1]
        self.assertTrue(expected_rows[TableName.CONTRIBUTOR] in contributor_rows)

        publication_rows = actual_rows[2][1]
        if expected_rows[TableName.PUBLICATION]:
            self.assertTrue(expected_rows[TableName.PUBLICATION] in publication_rows)


class SpecimenLibraryValidator(TransformerValidator):
    def validate(self, actual_rows: typing.Tuple, expected_rows: dict, bundle_type: BundleType):
        specimen_rows = actual_rows[0][1]
        self.assertTrue(expected_rows[TableName.SPECIMEN] in specimen_rows)

        library_rows = actual_rows[1][1]
        self.assertTrue(expected_rows[TableName.LIBRARY_PREPARATION] in library_rows)

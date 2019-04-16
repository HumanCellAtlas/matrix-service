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
        self.assertTrue(expected_rows[TableName.ANALYSIS] in analysis_rows)


class CellExpressionValidator(TransformerValidator):
    def validate(self, actual_rows: typing.Tuple, expected_rows: dict, bundle_type: BundleType):
        cell_rows = actual_rows[0][1]
        if bundle_type == BundleType.SS2:
            self.assertEqual(cell_rows[0], expected_rows[TableName.CELL])
        elif bundle_type == BundleType.CELLRANGER:
            self.assertTrue(expected_rows[TableName.CELL] in cell_rows)

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

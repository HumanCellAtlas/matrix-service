import unittest

from matrix.common import query_constructor


class TestFilterWhereConstruction(unittest.TestCase):

    def test_errors(self):
        """Test that correct exceptions are raised on bad filters."""

        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.filter_to_where({})

        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.filter_to_where("abcde")

        # Missing value
        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.filter_to_where(
                {"op": "<="})

        # Missing field
        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.filter_to_where(
                {"op": "<=", "value": 5})

        # Bad op
        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.filter_to_where(
                {"op": "xxx", "field": "bar", "value": "baz"})

        # in needs a list
        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.filter_to_where(
                {"op": "in", "field": "bar", "value": "baz"})

        # logical needs array
        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.filter_to_where(
                {"op": "and", "value": "baz"})

        # not takes one value
        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.filter_to_where(
                {"op": "not", "value": ["bar", "baz"]})

    def test_simple_comparison(self):

        filter_ = \
            {
                "op": "=",
                "field": "foo",
                "value": "bar"
            }
        expected_sql = "foo = 'bar'"
        self.assertEqual(query_constructor.filter_to_where(filter_), expected_sql)

        filter_ = \
            {
                "op": ">",
                "field": "baz",
                "value": 5
            }
        expected_sql = "baz > 5"
        self.assertEqual(query_constructor.filter_to_where(filter_), expected_sql)

    def test_in_comparison(self):

        filter_ = \
            {
                "op": "in",
                "field": "foo",
                "value": ["bar", "baz"]
            }
        expected_sql = "foo IN ('bar', 'baz')"
        self.assertEqual(query_constructor.filter_to_where(filter_), expected_sql)

        filter_ = \
            {
                "op": "in",
                "field": "qux",
                "value": [1, 'quuz', 3]
            }
        expected_sql = "qux IN (1, 'quuz', 3)"
        self.assertEqual(query_constructor.filter_to_where(filter_), expected_sql)

    def test_not(self):
        filter_ = \
            {
                "op": "not",
                "value": [{
                    "op": "in",
                    "field": "foo",
                    "value": ["bar", "baz"]
                }]
            }
        expected_sql = "NOT (foo IN ('bar', 'baz'))"
        self.assertEqual(query_constructor.filter_to_where(filter_), expected_sql)

    def test_and(self):
        filter_ = \
            {
                "op": "and",
                "value": [
                    {
                        "op": "in",
                        "field": "foo",
                        "value": ["bar", "baz"]
                    },
                    {
                        "op": ">",
                        "field": "qux",
                        "value": 5
                    },
                    {
                        "op": "=",
                        "field": "quuz",
                        "value": "thud"
                    }
                ]
            }
        expected_sql = "((foo IN ('bar', 'baz')) AND (qux > 5) AND (quuz = 'thud'))"
        self.assertEqual(query_constructor.filter_to_where(filter_), expected_sql)

    def test_or(self):
        filter_ = \
            {
                "op": "or",
                "value": [
                    {
                        "op": "in",
                        "field": "foo",
                        "value": ["bar", "baz"]
                    },
                    {
                        "op": ">",
                        "field": "qux",
                        "value": 5
                    },
                    {
                        "op": "=",
                        "field": "quuz",
                        "value": "thud"
                    }
                ]
            }
        expected_sql = "((foo IN ('bar', 'baz')) OR (qux > 5) OR (quuz = 'thud'))"
        self.assertEqual(query_constructor.filter_to_where(filter_), expected_sql)

    def test_nested(self):
        filter_ = \
            {
                "op": "or",
                "value": [
                    {
                        "op": "and",
                        "value": [
                            {
                                "op": "in",
                                "field": "num_field_1",
                                "value": [1, 2, 3, 4]
                            },
                            {
                                "op": ">",
                                "field": "num_field_2",
                                "value": 50
                            },
                            {
                                "op": "=",
                                "field": "quuz",
                                "value": "thud"
                            }
                        ]
                    },
                    {
                        "op": "not",
                        "value": [
                            {
                                "op": "in",
                                "field": "foo",
                                "value": ["bar", "baz"]
                            },
                        ]
                    },
                    {
                        "op": ">",
                        "field": "qux",
                        "value": 5
                    },
                    {
                        "op": "=",
                        "field": "quuz",
                        "value": "thud"
                    }
                ]
            }
        expected_sql = ("((((num_field_1 IN (1, 2, 3, 4)) AND (num_field_2 > 50) AND (quuz = 'thud'))) "
                        "OR (NOT (foo IN ('bar', 'baz'))) OR (qux > 5) OR (quuz = 'thud'))")
        self.assertEqual(query_constructor.filter_to_where(filter_), expected_sql)


class TestFeatureWhereConstruction(unittest.TestCase):

    def test_errors(self):

        with self.assertRaises(query_constructor.MalformedMatrixFeature):
            query_constructor.feature_to_where("foo")

    def test_gene_transcript(self):

        self.assertEqual(query_constructor.feature_to_where("gene"), "feature.isgene")
        self.assertEqual(query_constructor.feature_to_where("transcript"), "(NOT feature.isgene)")


class TestMatrixRequestQuery(unittest.TestCase):

    def test_errors(self):
        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.create_matrix_request_queries({}, ["specimen.organ"], "gene")

        with self.assertRaises(query_constructor.MalformedMatrixFeature):
            query_constructor.create_matrix_request_queries(
                {"op": "=", "field": "foo", "value": "bar"}, ["specimen.organ"], "baz")

    def test_transcript(self):
        filter_ = \
            {
                "op": "in",
                "field": "foo",
                "value": ["bar", "baz"]
            }

        fields = ["test.field"]
        feature = "transcript"

        queries = query_constructor.create_matrix_request_queries(filter_, fields, feature)
        expected_exp_query = """
UNLOAD ($$SELECT cell.cellkey, expression.featurekey, expression.exrpvalue
FROM expression
  LEFT OUTER JOIN feature on (expression.featurekey = feature.featurekey)
  INNER JOIN cell on (expression.cellkey = cell.cellkey)
  INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
  INNER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
  INNER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
  INNER JOIN project on (cell.projectkey = project.projectkey)
WHERE (NOT feature.isgene)
  AND expression.exprtype = 'Count'
  AND foo IN ('bar', 'baz')$$)
TO 's3://{results_bucket}/{request_id}/expression_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE
;
"""
        self.assertEqual(queries["expression_query"], expected_exp_query)

        expected_feature_query = """
UNLOAD ($$SELECT *
FROM feature
WHERE (NOT feature.isgene)$$)
to 's3://{results_bucket}/{request_id}/gene_metadata_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE;
"""
        self.assertEqual(queries["feature_query"], expected_feature_query)

    def test_nested(self):
        filter_ = \
            {
                "op": "or",
                "value": [
                    {
                        "op": "and",
                        "value": [
                            {
                                "op": "in",
                                "field": "num_field_1",
                                "value": [1, 2, 3, 4]
                            },
                            {
                                "op": ">",
                                "field": "num_field_2",
                                "value": 50
                            },
                            {
                                "op": "=",
                                "field": "quuz",
                                "value": "thud"
                            }
                        ]
                    },
                    {
                        "op": "not",
                        "value": [
                            {
                                "op": "in",
                                "field": "foo",
                                "value": ["bar", "baz"]
                            },
                        ]
                    },
                    {
                        "op": ">",
                        "field": "qux",
                        "value": 5
                    },
                    {
                        "op": "=",
                        "field": "quuz",
                        "value": "thud"
                    }
                ]
            }
        fields = ["test.field1", "test.field2"]
        feature = "gene"

        queries = query_constructor.create_matrix_request_queries(filter_, fields, feature)
        expected_cell_query = ("""
UNLOAD($$SELECT cell.cellkey, test.field1, test.field2
FROM cell
  LEFT OUTER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
  LEFT OUTER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
  LEFT OUTER JOIN project on (cell.projectkey = project.projectkey)
  INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
WHERE ((((num_field_1 IN (1, 2, 3, 4)) AND (num_field_2 > 50) AND (quuz = 'thud'))) OR (NOT (foo IN ('bar', 'baz'))) OR (qux > 5) OR (quuz = 'thud'))$$)"""  # noqa: E501
"""
TO 's3://{results_bucket}/{request_id}/cell_metadata_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE
;
""")
        self.assertEqual(queries["cell_query"], expected_cell_query)

        expected_exp_query = ("""
UNLOAD ($$SELECT cell.cellkey, expression.featurekey, expression.exrpvalue
FROM expression
  LEFT OUTER JOIN feature on (expression.featurekey = feature.featurekey)
  INNER JOIN cell on (expression.cellkey = cell.cellkey)
  INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
  INNER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
  INNER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
  INNER JOIN project on (cell.projectkey = project.projectkey)
WHERE feature.isgene
  AND expression.exprtype = 'Count'
  AND ((((num_field_1 IN (1, 2, 3, 4)) AND (num_field_2 > 50) AND (quuz = 'thud'))) OR (NOT (foo IN ('bar', 'baz'))) OR (qux > 5) OR (quuz = 'thud'))$$)"""  # noqa: E501
"""
TO 's3://{results_bucket}/{request_id}/expression_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE
;
""")
        self.assertEqual(queries["expression_query"], expected_exp_query)

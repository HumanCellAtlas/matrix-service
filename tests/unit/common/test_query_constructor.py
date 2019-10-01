import unittest

from matrix.common import constants
from matrix.common import query_constructor
from matrix.docker.query_runner import QueryType


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

        # and and or take two values
        with self.assertRaises(query_constructor.MalformedMatrixFilter):
            query_constructor.filter_to_where(
                {"op": "and", "value": [{"op": "=", "field": "foo", "value": "bar"}]})

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
TO 's3://{results_bucket}/{request_id}/{genus_species}/expression_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE
;
"""
        self.assertEqual(queries[QueryType.EXPRESSION], expected_exp_query)

        expected_feature_query = """
UNLOAD ($$SELECT *
FROM feature
WHERE (NOT feature.isgene)
  AND feature.genus_species = '{genus_species}'$$)
to 's3://{results_bucket}/{request_id}/{genus_species}/gene_metadata_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE;
"""
        self.assertEqual(queries[QueryType.FEATURE], expected_feature_query)

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
TO 's3://{results_bucket}/{request_id}/{genus_species}/cell_metadata_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE
;
""")
        self.assertEqual(queries[QueryType.CELL], expected_cell_query)

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
TO 's3://{results_bucket}/{request_id}/{genus_species}/expression_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE
;
""")
        self.assertEqual(queries[QueryType.EXPRESSION], expected_exp_query)


class TestNameConversion(unittest.TestCase):

    def test_field_conversion(self):
        filter_ = \
            {
                "op": "not",
                "value": [{
                    "op": "in",
                    "field": "foo",
                    "value": ["bar", "baz"]
                }]
            }
        fields = ["dss_bundle_fqid", "genes_detected",
                  "library_preparation_protocol.strand"]
        feature = "gene"
        queries = query_constructor.create_matrix_request_queries(filter_, fields, feature)

        expected_cell_query = ("""
UNLOAD($$SELECT cell.cellkey, analysis.bundle_fqid, cell.genes_detected, library_preparation.strand
FROM cell
  LEFT OUTER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
  LEFT OUTER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
  LEFT OUTER JOIN project on (cell.projectkey = project.projectkey)
  INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
WHERE NOT (foo IN ('bar', 'baz'))$$)
TO 's3://{results_bucket}/{request_id}/{genus_species}/cell_metadata_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE
;
""")
        self.assertEqual(queries[QueryType.CELL], expected_cell_query)

    def test_filter_conversion(self):
        filter_ = \
            {
                "op": "and",
                "value": [
                    {
                        "op": "=",
                        "field": "project.project_core.project_short_name",
                        "value": "project1"
                    },
                    {
                        "op": ">",
                        "field": "genes_detected",
                        "value": 1000
                    }
                ]
            }

        feature = "transcript"
        queries = query_constructor.create_matrix_request_queries(
            filter_, constants.DEFAULT_FIELDS, feature)

        expected_cell_query = ("""
UNLOAD($$SELECT cell.cellkey, cell.cell_suspension_id, cell.genes_detected, cell.file_uuid, cell.file_version, cell.total_umis, cell.emptydrops_is_cell, cell.barcode, specimen.*, library_preparation.*, project.*, analysis.*"""  # noqa: E501
"""
FROM cell
  LEFT OUTER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
  LEFT OUTER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
  LEFT OUTER JOIN project on (cell.projectkey = project.projectkey)
  INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
WHERE ((project.short_name = 'project1') AND (cell.genes_detected > 1000))$$)
TO 's3://{results_bucket}/{request_id}/{genus_species}/cell_metadata_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE
;
""")
        self.assertEqual(queries[QueryType.CELL], expected_cell_query)

        expected_exp_query = ("""
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
  AND ((project.short_name = 'project1') AND (cell.genes_detected > 1000))$$)
TO 's3://{results_bucket}/{request_id}/{genus_species}/expression_'
IAM_ROLE '{iam_role}'
GZIP
MANIFEST VERBOSE
;
""")
        self.assertEqual(queries[QueryType.EXPRESSION], expected_exp_query)


class TestDetailQuery(unittest.TestCase):

    def test_cell_numeric(self):
        query = query_constructor.create_field_detail_query(
            "cell.genes_detected",
            "cell",
            "cellkey",
            "numeric")

        expected_sql = """
SELECT MIN(cell.genes_detected), MAX(cell.genes_detected)
FROM cell 
;
"""  # noqa: W291
        self.assertEqual(query, expected_sql)

    def test_cell_categorical(self):

        query = query_constructor.create_field_detail_query(
            "cell.cell_suspension_id",
            "cell",
            "cellkey",
            "categorical")

        expected_sql = """
SELECT cell.cell_suspension_id, COUNT(cell.cellkey)
FROM cell 
GROUP BY cell.cell_suspension_id
;
"""  # noqa: W291
        self.assertEqual(query, expected_sql)

    def test_noncell_categorical(self):

        query = query_constructor.create_field_detail_query(
            "analysis.protocol",
            "analysis",
            "analysiskey",
            "categorical")

        expected_sql = """
SELECT analysis.protocol, COUNT(cell.cellkey)
FROM cell LEFT OUTER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
GROUP BY analysis.protocol
;
"""
        self.assertEqual(query, expected_sql)


class TestFormatStrList(unittest.TestCase):

    def test_multiple_values(self):

        values = ["id1.version", "id2.version"]
        formatted_values = "('id1.version', 'id2.version')"

        self.assertEqual(query_constructor.format_str_list(values), formatted_values)

    def test_single_value(self):

        values = ["id1.version"]
        formatted_values = "('id1.version')"

        self.assertEqual(query_constructor.format_str_list(values), formatted_values)


class TestHasGenusSpecies(unittest.TestCase):

    def test_simple(self):
        filter_ = \
            {
                "op": "=",
                "field": "specimen_from_organism.genus_species.ontology_label",
                "value": "Homo sapiens"
            }
        self.assertTrue(query_constructor.has_genus_species_term(filter_))

        filter_ = \
            {
                "op": "!=",
                "field": "specimen_from_organism.genus_species.ontology",
                "value": "NCBITaxon:9606"
            }
        self.assertTrue(query_constructor.has_genus_species_term(filter_))

        filter_ = \
            {
                "op": ">",
                "field": "genes_detected",
                "value": 100
            }
        self.assertFalse(query_constructor.has_genus_species_term(filter_))

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
                                "op": "!=",
                                "field": "specimen_from_organism.genus_species.ontology",
                                "value": "NCBITaxon:9606"
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
        self.assertTrue(query_constructor.has_genus_species_term(filter_))

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
        self.assertFalse(query_constructor.has_genus_species_term(filter_))


class TestSpeciesifyFilter(unittest.TestCase):

    def test_comparison(self):

        filter_ = {
            "op": ">",
            "field": "genes_detected",
            "value": 1000
        }

        speciesified_filter = query_constructor.speciesify_filter(filter_, constants.GenusSpecies.HUMAN)
        expected_filter = {
            "op": "and",
            "value": [
                {
                    "op": "=",
                    'field': 'specimen_from_organism.genus_species.ontology_label',
                    "value": constants.GenusSpecies.HUMAN.value
                },
                {
                    "op": ">",
                    "field": "genes_detected",
                    "value": 1000
                }
            ]
        }
        self.assertDictEqual(speciesified_filter, expected_filter)

    def test_logical(self):
        filter_ = {
            "op": "or",
            "value": [
                {
                    "op": ">",
                    "field": "genes_detected",
                    "value": 1000
                },
                {
                    "op": "=",
                    "field": "foo",
                    "value": "bar"
                }
            ]
        }

        speciesified_filter = query_constructor.speciesify_filter(filter_, constants.GenusSpecies.MOUSE)
        expected_filter = {
            "op": "and",
            "value": [
                {
                    "op": "=",
                    'field': 'specimen_from_organism.genus_species.ontology_label',
                    "value": constants.GenusSpecies.MOUSE.value
                },
                {
                    "op": "or",
                    "value": [
                        {
                            "op": ">",
                            "field": "genes_detected",
                            "value": 1000
                        },
                        {
                            "op": "=",
                            "field": "foo",
                            "value": "bar"
                        }
                    ]
                }
            ]
        }
        self.assertDictEqual(speciesified_filter, expected_filter)

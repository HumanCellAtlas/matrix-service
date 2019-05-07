import unittest

from matrix.common import query_constructor


class TestWhereConstruction(unittest.TestCase):

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

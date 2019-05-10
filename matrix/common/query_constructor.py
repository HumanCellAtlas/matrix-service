COMPARISON_OPERATORS = [
    "=", ">=", "<=", "<", ">", "!=", "in"
]


LOGICAL_OPERATORS = ["and", "or", "not"]

DEFAULT_FIELDS = ["cell.cell_suspension_id", "cell.genes_detected", "specimen.*",
                  "library_preparation.*", "project.*", "analysis.*"]

DEFAULT_FEATURE = "gene"

EXPRESSION_QUERY_TEMPLATE = """
UNLOAD ($$SELECT cell.cellkey, expression.featurekey, expression.exrpvalue
FROM expression
  LEFT OUTER JOIN feature on (expression.featurekey = feature.featurekey)
  INNER JOIN cell on (expression.cellkey = cell.cellkey)
  INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
  INNER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
  INNER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
  INNER JOIN project on (cell.projectkey = project.projectkey)
WHERE {feature_where_clause}
  AND expression.exprtype = 'Count'
  AND {cell_where_clause}$$)
TO 's3://{{results_bucket}}/{{request_id}}/expression_'
IAM_ROLE '{{iam_role}}'
GZIP
MANIFEST VERBOSE
;
"""

CELL_QUERY_TEMPLATE = """
UNLOAD($$SELECT cell.cellkey, {fields}
FROM cell
  LEFT OUTER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
  LEFT OUTER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
  LEFT OUTER JOIN project on (cell.projectkey = project.projectkey)
  INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
WHERE {cell_where_clause}$$)
TO 's3://{{results_bucket}}/{{request_id}}/cell_metadata_'
IAM_ROLE '{{iam_role}}'
GZIP
MANIFEST VERBOSE
;
"""

FEATURE_QUERY_TEMPLATE = """
UNLOAD ($$SELECT *
FROM feature
WHERE {feature_where_clause}$$)
to 's3://{{results_bucket}}/{{request_id}}/gene_metadata_'
IAM_ROLE '{{iam_role}}'
GZIP
MANIFEST VERBOSE;
"""

# Query templates for requests to /filter/... and /fields/...
FIELD_DETAIL_CATEGORICAL_QUERY_TEMPLATE = """
SELECT {fq_field_name}, COUNT(cell.cellkey)
FROM cell
  LEFT OUTER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
  LEFT OUTER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
  LEFT OUTER JOIN project on (cell.projectkey = project.projectkey)
  INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
GROUP BY {fq_field_name}
;
"""

FIELD_DETAIL_NUMERIC_QUERY_TEMPLATE = """
SELECT MIN({fq_field_name}), MAX({fq_field_name})
FROM cell
  LEFT OUTER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
  LEFT OUTER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
  LEFT OUTER JOIN project on (cell.projectkey = project.projectkey)
  INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
;
"""


class MalformedMatrixFilter(Exception):
    pass


class MalformedMatrixFeature(Exception):
    pass


def create_matrix_request_queries(filter_, fields, feature):

    cell_where_clause = filter_to_where(filter_)
    feature_where_clause = feature_to_where(feature)

    expression_query = EXPRESSION_QUERY_TEMPLATE.format(
        feature_where_clause=feature_where_clause,
        cell_where_clause=cell_where_clause)

    cell_query = CELL_QUERY_TEMPLATE.format(
        fields=', '.join(fields),
        cell_where_clause=cell_where_clause)

    feature_query = FEATURE_QUERY_TEMPLATE.format(feature_where_clause=feature_where_clause)

    return {
        "expression_query": expression_query,
        "cell_query": cell_query,
        "feature_query": feature_query
    }


def feature_to_where(matrix_feature):
    """Build the WHERE clause for the features."""

    if matrix_feature == "gene":
        return "feature.isgene"
    elif matrix_feature == "transcript":
        return "(NOT feature.isgene)"
    else:
        raise MalformedMatrixFeature(f"Unknown feature type {matrix_feature}")


def filter_to_where(matrix_filter):
    """Build a WHERE clause for the matrix request SQL query out of the matrix
    filter object.
    """

    try:
        op = matrix_filter["op"]
    except Exception as exc:
        raise MalformedMatrixFilter("Could not retrieve filter op") from exc

    try:
        value = matrix_filter["value"]
    except Exception as exc:
        raise MalformedMatrixFilter("Could not retrieve filter value") from exc

    if op in COMPARISON_OPERATORS:

        try:
            field = matrix_filter["field"]
        except Exception as exc:
            raise MalformedMatrixFilter("Could not retrieve filter field") from exc

        if op == 'in':
            if not isinstance(value, (list, tuple)):
                raise MalformedMatrixFilter("The 'in' operator requires an array value")
            value = ', '.join([f"'{el}'" if isinstance(el, str) else str(el) for el in value])
            value = '(' + value + ')'
        else:
            if isinstance(value, str):
                value = f"'{value}'"

        return f"{field} {op.upper()} {value}"

    elif op in LOGICAL_OPERATORS:

        if not isinstance(value, (list, tuple)):
            raise MalformedMatrixFilter("Logical operators accept a value array")

        if op == "not":
            if len(value) != 1:
                raise MalformedMatrixFilter("not operator accepts an array of length 1")
            return f"NOT ({filter_to_where(value[0])})"
        else:
            if len(value) < 2:
                raise MalformedMatrixFilter(
                    "(and, or) operators accept an array of length at least 2")

            return '(' + f" {op.upper()} ".join([
                f"({filter_to_where(v)})" for v in value]) + ')'
    else:
        raise MalformedMatrixFilter(f"Invalid op: {op}")

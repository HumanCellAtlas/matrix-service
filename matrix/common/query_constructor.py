"""Methods and templates for redshift queries."""

import typing

from matrix.common import constants

COMPARISON_OPERATORS = [
    "=", ">=", "<=", "<", ">", "!=", "in"
]


LOGICAL_OPERATORS = ["and", "or", "not"]

DEFAULT_FIELDS = ["cell.cell_suspension_id", "cell.genes_detected", "specimen.*",
                  "library_preparation.*", "project.*", "analysis.*"]

DEFAULT_FEATURE = constants.MatrixFeature.GENE.value

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
FROM cell {join}
GROUP BY {fq_field_name}
;
"""

FIELD_DETAIL_NUMERIC_QUERY_TEMPLATE = """
SELECT MIN({fq_field_name}), MAX({fq_field_name})
FROM cell {join}
;
"""

FIELD_DETAIL_JOIN = "LEFT OUTER JOIN {table_name} on (cell.{primary_key} = {table_name}.{primary_key})"


class MalformedMatrixFilter(Exception):
    """Error indicating something wrong with a matrix filter."""
    pass


class MalformedMatrixFeature(Exception):
    """Error indicating something wrong with a matrix feature."""
    pass


def _get_internal_name(metadata_name):
    """Translate the external field name used in the matrix service API to
    the internal table and field name used by redshift.

    If the translation doesn't work, just return the original name.
    """

    try:
        column_name = constants.METADATA_FIELD_TO_TABLE_COLUMN[metadata_name]
        table_name = constants.TABLE_COLUMN_TO_TABLE[column_name]
        fq_name = table_name + '.' + column_name
        return fq_name
    except KeyError:
        return metadata_name


def create_field_detail_query(fq_field_name, table_name, primary_key, field_type):

    if table_name == "cell":
        join = ""
    else:
        join = FIELD_DETAIL_JOIN.format(table_name=table_name, primary_key=primary_key)

    if field_type == "categorical":
        query = FIELD_DETAIL_CATEGORICAL_QUERY_TEMPLATE.format(
            fq_field_name=fq_field_name,
            join=join)
    elif field_type == "numeric":
        query = FIELD_DETAIL_NUMERIC_QUERY_TEMPLATE.format(
            fq_field_name=fq_field_name,
            join=join)
    else:
        raise ValueError(f"Invalid field type {field_type}, expecting categorical or numeric")

    return query


def translate_filters(filter_: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """Translate the filter fields specified by the user into their internal
    names.

    The metadata names that the matrix service presents to users are taken from
    the project metadata tsvs. These names are different from the table and column
    names inside the matrix service. This function takes a filter with a field
    like "project.project_core.project_short_name" and translates it to
    "project.short_name" so the query will execute correctly.
    """

    new_filter = dict(filter_)
    if "field" in filter_:
        fq_name = _get_internal_name(filter_["field"])
        new_filter["field"] = fq_name
    else:
        try:
            new_values = [translate_filters(f) for f in filter_["value"]]
            new_filter["value"] = new_values
        except KeyError:
            pass

    return new_filter


def translate_fields(fields: typing.List[str]) -> typing.List[str]:
    """Translate field list from external metadata field names to internal
    redshift names.
    """

    new_fields = []
    for field in fields:
        fq_name = _get_internal_name(field)
        new_fields.append(fq_name)
    return new_fields


def create_matrix_request_queries(filter_: typing.Dict[str, typing.Any],
                                  fields: typing.List[str],
                                  feature: str) -> typing.Dict[str, str]:
    """Based on values from the matrix request, create an appropriate
    set of redshift queries to serve the request.
    """

    cell_where_clause = filter_to_where(translate_filters(filter_))
    feature_where_clause = feature_to_where(feature)

    expression_query = EXPRESSION_QUERY_TEMPLATE.format(
        feature_where_clause=feature_where_clause,
        cell_where_clause=cell_where_clause)

    cell_query = CELL_QUERY_TEMPLATE.format(
        fields=', '.join(translate_fields(fields)),
        cell_where_clause=cell_where_clause)

    feature_query = FEATURE_QUERY_TEMPLATE.format(feature_where_clause=feature_where_clause)

    return {
        "expression_query": expression_query,
        "cell_query": cell_query,
        "feature_query": feature_query
    }


def feature_to_where(matrix_feature: str) -> str:
    """Build the WHERE clause for the features."""

    if matrix_feature == constants.MatrixFeature.GENE.value:
        return "feature.isgene"
    elif matrix_feature == constants.MatrixFeature.TRANSCRIPT.value:
        return "(NOT feature.isgene)"
    else:
        raise MalformedMatrixFeature(f"Unknown feature type {matrix_feature}")


def filter_to_where(matrix_filter: typing.Dict[str, typing.Any]) -> str:
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


def format_str_list(values: typing.Iterable[str]) -> str:
    """
    Formats a list of strings into a query compatible string
    :param values: list of strings
    :return: stringified list
    """
    return '(' + ', '.join("'" + str(v) + "'" for v in values) + ')'

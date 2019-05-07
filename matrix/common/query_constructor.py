COMPARISON_OPERATORS = [
    "=", ">=", "<=", "<", ">", "!=", "in"
]


LOGICAL_OPERATORS = ["and", "or", "not"]


class MalformedMatrixFilter(Exception):
    pass


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

from enum import Enum


class MatrixFormat(Enum):
    """
    Supported expression matrix output formats.
    Keep up-to-date with config/matrix-api.yml (MatrixFormat)
    """
    LOOM = "loom"
    CSV = "csv"
    MTX = "mtx"


class MatrixRequestStatus(Enum):
    COMPLETE = "Complete"
    IN_PROGRESS = "In Progress"
    FAILED = "Failed"

from enum import Enum


class MatrixFormat(Enum):
    ZARR = "zarr"
    LOOM = "loom"
    CSV = "csv"


class MatrixRequestStatus(Enum):
    COMPLETE = "Complete"
    IN_PROGRESS = "In progress"
    FAILED = "Failed"

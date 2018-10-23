from enum import Enum


class MatrixFormat(Enum):
    ZARR = "zarr"
    LOOM = "loom"
    CSV = "csv"


class MatrixRequestStatus(Enum):
    COMPLETE = "Complete"
    IN_PROGRESS = "In Progress"
    FAILED = "Failed"


class ZarrayName(Enum):
    """
    Names of expression matrix zarrays
    """
    EXPRESSION = "expression"
    CELL_ID = "cell_id"
    CELL_METADATA_NUMERIC = "cell_metadata_numeric"
    CELL_METADATA_STRING = "cell_metadata_string"
    CELL_METADATA_NUMERIC_NAME = "cell_metadata_numeric_name"
    CELL_METADATA_STRING_NAME = "cell_metadata_string_name"
    GENE_ID = "gene_id"
    GENE_METADATA = "gene_metadata"
    GENE_METADATA_NAME = "gene_metadata_name"

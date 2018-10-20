from enum import Enum


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


class ZarrStore:
    """
    Abstraction for reading and writing remote expression matrix zarr stores.
    """
    @property
    def expression(self):
        raise NotImplementedError

    @property
    def cell_id(self):
        raise NotImplementedError

    @property
    def cell_metadata_numeric(self):
        raise NotImplementedError

    @property
    def cell_metadata_string(self):
        raise NotImplementedError

    @property
    def cell_metadata_numeric_name(self):
        raise NotImplementedError

    @property
    def cell_metadata_string_name(self):
        raise NotImplementedError

    @property
    def gene_id(self):
        raise NotImplementedError

    @property
    def gene_metadata(self):
        raise NotImplementedError

    @property
    def gene_metadata_name(self):
        raise NotImplementedError

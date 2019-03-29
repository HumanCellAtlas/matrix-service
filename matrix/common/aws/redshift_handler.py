import psycopg2 as pg
import typing
from enum import Enum

from matrix.common.config import MatrixRedshiftConfig


class TableName(Enum):
    """
    Redshift table names.
    """
    FEATURE = "feature"
    ANALYSIS = "analysis"
    SPECIMEN = "specimen"
    LIBRARY_PREPARATION = "library_preparation"
    PROJECT = "project"
    PUBLICATION = "publication"
    CONTRIBUTOR = "contributor"
    CELL = "cell"
    EXPRESSION = "expression"


class RedshiftHandler:
    """
    Interface for interacting with redshift cluster.
    """
    PRIMARY_KEY = {
        TableName.FEATURE: "featurekey",
        TableName.ANALYSIS: "analysiskey",
        TableName.SPECIMEN: "specimenkey",
        TableName.LIBRARY_PREPARATION: "librarykey",
        TableName.PROJECT: "projectkey",
        TableName.PUBLICATION: "projectkey",
        TableName.CONTRIBUTOR: "projectkey",
        TableName.CELL: "cellkey",
        TableName.EXPRESSION: "cellkey",
    }

    def __init__(self):
        self.redshift_config = MatrixRedshiftConfig()

    @property
    def database_uri(self):
        return self.redshift_config.database_uri

    def transaction(self, queries: typing.List[str], return_results=False):
        results = []
        conn = pg.connect(self.database_uri)
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query)
        if return_results:
            results = cursor.fetchall()
        conn.commit()
        conn.close()
        return results

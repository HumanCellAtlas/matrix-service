import psycopg2 as pg

from matrix.common.config import MatrixRedshiftConfig


class RedshiftHandler:
    """
    Interface for interacting with redshift cluster.
    """

    def __init__(self):
        self.redshift_config = MatrixRedshiftConfig()

    @property
    def database_uri(self):
        return self.redshift_config.database_uri

    def run_query(self, query):
        conn = pg.connect(self.database_uri)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.close()

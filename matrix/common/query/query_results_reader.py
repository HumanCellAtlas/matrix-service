import json

import s3fs

from matrix.common import constants


class MatrixQueryResultsNotFound(Exception):
    """Error indicating access to query results not found in S3"""
    pass


class QueryResultsReader:
    """
    Provides an abstract API to load large Redshift query results
    stored in S3 (produced by Redshift UNLOAD) into memory.

    load_results: Loads all results into memory
    load_slice: Loads results at the given slice index
    """
    def __init__(self, s3_manifest_key):
        self._s3fs = s3fs.S3FileSystem()

        self.s3_manifest_key = s3_manifest_key
        self.manifest = self._parse_manifest(s3_manifest_key)

    def load_results(self):
        """
        Loads all query results of the provided manifest into memory.
        """
        raise NotImplementedError()

    def load_slice(self, slice_idx):
        """
        Loads a slice of query results okf the provided manifest into memory.
        :param slice_idx: Slice to load
        """
        raise NotImplementedError()

    def _parse_manifest(self, manifest_key):
        """Parse a manifest file produced by a Redshift UNLOAD query.

        Args:
            manifest_key: S3 location of the manifest file.

        Returns:
            dict with three keys:
                "columns": the column headers for the tables
                "part_urls": full S3 urls for the files containing results from each
                    Redshift slice
                "record_count": total number of records returned by the query
        """
        try:
            manifest = json.load(self._s3fs.open(manifest_key))
        except FileNotFoundError:
            raise MatrixQueryResultsNotFound(f"Unable to locate query results at {manifest_key}.")

        return {
            "columns": [e["name"] for e in manifest["schema"]["elements"]],
            "part_urls": [e["url"] for e in manifest["entries"] if e["meta"]["record_count"]],
            "record_count": manifest["meta"]["record_count"]
        }

    @staticmethod
    def _map_columns(cols: list):
        """
        Maps Redshift column names to schema friendly metadata field names, if available
        :param cols: List of table column names to map
        :return: List of schema friendly metadata field names
        """
        return [constants.TABLE_COLUMN_TO_METADATA_FIELD[col]
                if col in constants.TABLE_COLUMN_TO_METADATA_FIELD else col
                for col in cols]

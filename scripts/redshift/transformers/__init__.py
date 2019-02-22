import os
import typing

from ..init_cluster import STAGING_DIRECTORY


class MetadataToPsvTransformer:
    """
    Abstract class for transforming DSS metadata to PSV rows.
    """
    PSV_EXT = ".psv"
    OUTPUT_DIR = os.path.join(STAGING_DIRECTORY, 'output')

    def transform(self, bundle_dir: str):
        """
        Parses a bundle's metadata (JSON) and writes rows to corresponding PSV file(s).
        :param bundle_dir: Local path to bundle contents
        """
        self._write_rows_to_psvs(*self._parse_from_metadatas(bundle_dir))

    def _parse_from_metadatas(self, bundle_dir: str):
        """
        Parses JSON metadata for a bundle into set(s) of PSV rows.
        :param bundle_dir: Local path to bundle contents
        """
        raise NotImplementedError

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        """
        Writes row(s) to specified PSV file(s).

        :param args: n Tuples (TableName, Set(Tuple)) where
                TableName: the table to write to and
                Set(Tuple): A Tuple represents a row to write
        :return: None
        """
        for arg in args:
            table = arg[0]
            rows = arg[1]
            out_file = MetadataToPsvTransformer.OUTPUT_DIR + table.value + MetadataToPsvTransformer.PSV_EXT

            with open(out_file, 'a') as fh:
                for row in rows:
                    fh.write(row)

    @staticmethod
    def _generate_psv_row(*args):
        return '|'.join(args)

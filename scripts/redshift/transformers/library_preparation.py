import json
import pathlib
import typing
from threading import Lock

from . import MetadataToPsvTransformer
from ..init_cluster import TableName


class LibraryTransformer(MetadataToPsvTransformer):
    WRITE_LOCK = Lock()

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with LibraryTransformer.WRITE_LOCK:
            super(LibraryTransformer, self)._write_rows_to_psvs(*args)

    def _parse_from_metadatas(self, bundle_dir):
        libraries = set()
        p = pathlib.Path(bundle_dir)

        for path_to_json in p.glob("**/library_preparation_protocol_*.json"):
            library_dict = json.load(open(path_to_json))

            key = library_dict["provenance"]["document_id"]
            input_nucleic_acid = library_dict.get("input_nucleic_acid_molecule", {}).get("ontology", "").upper()
            construction_approach = library_dict.get("library_construction_approach", {}).get("ontology", "").upper()
            end_bias = library_dict.get("end_bias", "")
            strand = library_dict.get("strand", "")

            libraries.add(self._generate_psv_row(key, input_nucleic_acid, construction_approach, end_bias, strand))

        return (TableName.LIBRARY_PREPARATION, libraries),

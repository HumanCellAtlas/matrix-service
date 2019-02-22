import json
import pathlib
import typing
from threading import Lock

from . import MetadataToPsvTransformer
from ..init_cluster import TableName


class AnalysisTransformer(MetadataToPsvTransformer):
    WRITE_LOCK = Lock()

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with AnalysisTransformer.WRITE_LOCK:
            super(AnalysisTransformer, self)._write_rows_to_psvs(*args)

    def _parse_from_metadatas(self, bundle_dir):
        analyses = set()
        p = pathlib.Path(bundle_dir)

        for path_to_json in p.glob("**/analysis_protocol_*.json"):
            analysis_dict = json.load(open(path_to_json))

            key = analysis_dict["provenance"]["document_id"]
            bundle_uuid = path_to_json.parts[-2]
            protocol = analysis_dict["protocol_core"]["protocol_id"]
            awg_disposition = "blessed" if protocol.startswith("smartseq2") else "community"

            analyses.add(self._generate_psv_row(key, bundle_uuid, protocol, awg_disposition))

        return (TableName.ANALYSIS, analyses),

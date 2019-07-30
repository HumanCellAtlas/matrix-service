import json
import pathlib
import typing
from threading import Lock

from . import MetadataToPsvTransformer
from matrix.common.aws.redshift_handler import TableName


class AnalysisTransformer(MetadataToPsvTransformer):
    """Reads analysis metadata and writes out rows for analysis table in PSV format."""
    WRITE_LOCK = Lock()

    def __init__(self, staging_dir):
        super(AnalysisTransformer, self).__init__(staging_dir)

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with AnalysisTransformer.WRITE_LOCK:
            super(AnalysisTransformer, self)._write_rows_to_psvs(*args)

    def _parse_from_metadatas(self, search_dir):
        analyses = set()
        p = pathlib.Path(search_dir)

        for path_to_json in p.glob("**/analysis_protocol_*.json"):
            analysis_dict = json.load(open(path_to_json))

            key = analysis_dict["provenance"]["document_id"]
            bundle_fqid = path_to_json.parts[-2]
            bundle_uuid, bundle_version = bundle_fqid.split(".", 1)
            protocol = analysis_dict["protocol_core"]["protocol_id"]
            awg_disposition = "blessed" if protocol.startswith("smartseq2") else "community"

            analyses.add(self._generate_psv_row(key, bundle_fqid, bundle_uuid, bundle_version,
                                                protocol, awg_disposition))

        return (TableName.ANALYSIS, analyses),

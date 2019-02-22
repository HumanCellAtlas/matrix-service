import json
import pathlib
import typing
from threading import Lock

from . import MetadataToPsvTransformer
from ..init_cluster import TableName


class ProjectPublicationContributorTransformer(MetadataToPsvTransformer):
    WRITE_LOCK = Lock()

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with ProjectPublicationContributorTransformer.WRITE_LOCK:
            super(ProjectPublicationContributorTransformer, self)._write_rows_to_psvs(*args)

    def _parse_from_metadatas(self, bundle_dir):
        projects = set()
        contributors = set()
        publications = set()

        p = pathlib.Path(bundle_dir)

        for path_to_json in p.glob("**/project_*.json"):
            project_dict = json.load(open(path_to_json))

            key = project_dict["provenance"]["document_id"]
            title = project_dict["project_core"]["project_title"]
            short_name = project_dict["project_core"]["project_short_name"]

            if "_test_" in short_name:
                continue

            projects.add(self._generate_psv_row(key, short_name, title))

            [contributors.add(self._generate_psv_row(key, c["contact_name"], c.get("institution")))
             for c in project_dict.get("contributors", [])]

            [publications.add(self._generate_psv_row(key, p.get("publication_title"), p.get("doi")))
             for p in project_dict.get("publications", [])]

        return ((TableName.PROJECT, projects),
                (TableName.CONTRIBUTOR, contributors),
                (TableName.PUBLICATION, publications))

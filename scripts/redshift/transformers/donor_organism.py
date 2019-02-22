import json
import pathlib
import typing
from threading import Lock

from . import MetadataToPsvTransformer
from ..init_cluster import TableName


class DonorTransformer(MetadataToPsvTransformer):
    WRITE_LOCK = Lock()

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with DonorTransformer.WRITE_LOCK:
            super(DonorTransformer, self)._write_rows_to_psvs(*args)

    def _parse_from_metadatas(self, bundle_dir):
        donors = set()
        p = pathlib.Path(bundle_dir)

        for path_to_json in p.glob("**/donor_organism_*.json"):
            donor_dict = json.load(open(path_to_json))
            key = donor_dict["provenance"]["document_id"]

            if "DEMO" in donor_dict["biomaterial_core"].get("biomaterial_id", ""):
                continue

            genus_species_list = donor_dict.get("genus_species", [])
            if genus_species_list:
                genus_species = genus_species_list[0].get("ontology", "").upper()
            else:
                genus_species = ""

            ethnicity_list = donor_dict.get("human_specific", {}).get("ethnicity", [])
            if ethnicity_list:
                ethnicity = ethnicity_list[0].get("ontology").upper()
            else:
                ethnicity = ""

            disease_list = donor_dict.get("diseases", [])
            if disease_list:
                disease = disease_list[0].get("ontology").upper()
            else:
                disease = ""

            development_stage = donor_dict.get("development_stage", {}).get("ontology", "").upper()

            donors.add(self._generate_psv_row(key, genus_species, ethnicity, disease, development_stage))

        return (TableName.DONOR_ORGANISM, donors),

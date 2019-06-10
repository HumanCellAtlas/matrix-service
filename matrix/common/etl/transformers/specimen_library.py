import glob
import json
import os
import pathlib
import re
import typing
from threading import Lock

import requests
from humancellatlas.data.metadata.api import lookup

from . import MetadataToPsvTransformer
from matrix.common.aws.redshift_handler import TableName


class SpecimenLibraryTransformer(MetadataToPsvTransformer):
    """
    Reads specimen and library preparation metadata and writes out rows for
    specimen and library_preparation tables in PSV format.
    """
    WRITE_LOCK = Lock()

    def __init__(self, staging_dir):
        super(SpecimenLibraryTransformer, self).__init__(staging_dir)

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with SpecimenLibraryTransformer.WRITE_LOCK:
            super(SpecimenLibraryTransformer, self)._write_rows_to_psvs(*args)

    def _parse_from_metadatas(self, search_dir: str):
        p = pathlib.Path(search_dir)

        specimen_infos = []
        for specimen_json_path in p.glob("**/specimen_from_organism_*.json"):
            specimen_info = self.parse_specimen_json(os.path.dirname(specimen_json_path))
            if specimen_info:
                specimen_infos.append(specimen_info)

        library_infos = []
        for library_json_path in p.glob("**/library_preparation_protocol_*.json"):
            library_info = self.parse_library_json(library_json_path)
            if library_info:
                library_infos.append(library_info)

        odict = self.create_ontology_resolver(specimen_infos, library_infos)

        specimen_data = set()
        for specimen_info in specimen_infos:
            specimen_data.add(
                self._generate_psv_row(specimen_info['key'],
                                       specimen_info['genus_species'],
                                       odict.get(specimen_info['genus_species'], ""),
                                       specimen_info['ethnicity'],
                                       odict.get(specimen_info["ethnicity"], ""),
                                       specimen_info['disease'],
                                       odict.get(specimen_info["disease"], ""),
                                       specimen_info['development_stage'],
                                       odict.get(specimen_info["development_stage"], ""),
                                       specimen_info["organ"],
                                       odict.get(specimen_info["organ"], ""),
                                       specimen_info["organ_parts"],
                                       odict.get(specimen_info["organ_parts"], "")))

        library_data = set()
        for library_info in library_infos:
            library_data.add(
                self._generate_psv_row(library_info['key'],
                                       library_info['input_nucleic_acid'],
                                       odict.get(library_info["input_nucleic_acid"], ""),
                                       library_info['construction_method'],
                                       odict.get(library_info['construction_method'], ""),
                                       library_info['end_bias'],
                                       library_info['strand']))

        return (TableName.SPECIMEN, specimen_data), (TableName.LIBRARY_PREPARATION, library_data)

    def resolve_ontology_ols(self, term):
        """Get the ontology label for a term using the OLS API."""

        special_case = {
            "NCBITAXON": "NCBITaxon",
            "HSAPDV": "HsapDv"
        }

        uri_templates = [
            ("http://www.ebi.ac.uk/ols/api/ontologies/{lontology}/terms/"
             "http%253A%252F%252Fpurl.obolibrary.org%252Fobo%252F{ontology}_{term_id}"),
            ("http://www.ebi.ac.uk/ols/api/ontologies/{lontology}/terms/"
             "http%253A%252F%252Fwww.ebi.ac.uk%252F{lontology}%252F{ontology}_{term_id}")
        ]
        try:
            ontology, term_id = term.split(":")
        except ValueError:
            return None

        ontology = special_case.get(ontology, ontology)

        # Match a trailing comment like "... (organoid)" or "... (cell line)". If there
        # is a match, strip it off and hold on to it.
        special_match = re.match("(.*)(\ \(.*\))", term_id)
        if special_match:
            is_special = True
            term_id = special_match.groups()[0]
            special_type = special_match.groups()[1]
        else:
            is_special = False

        label = None
        for uri_template in uri_templates:
            uri = uri_template.format(ontology=ontology, lontology=ontology.lower(),
                                      term_id=term_id)
            print("uri:", uri)
            resp = requests.get(uri)
            if "label" in resp.json():
                label = resp.json()["label"]
                if is_special:
                    label += special_type
                break
        return label

    def create_ontology_resolver(self, *metadata_dict_lists):
        """Look through all the gathered metadata and create a dict that maps
        from ontology term to ontology label.
        """

        candidate_terms = set()
        for metadata_dict_list in metadata_dict_lists:
            for metadata_dict in metadata_dict_list:
                for k, v in metadata_dict.items():
                    if k == "key":
                        continue
                    if v:
                        candidate_terms.add(v)

        resolver = {}
        for candidate_term in candidate_terms:
            label = self.resolve_ontology_ols(candidate_term)
            if label:
                resolver[candidate_term] = label

        return resolver

    def parse_specimen_json(self, bundle_path):
        specimen_json_paths = glob.glob(
            os.path.join(bundle_path, "specimen_from_organism_*.json"))
        donor_json_paths = glob.glob(
            os.path.join(bundle_path, "donor_organism_*.json"))

        genera = set()
        ethnicities = set()
        diseases = set()
        dev_stages = set()
        specimenkeys = []

        for specimen_json_path in specimen_json_paths:
            specimen_dict = json.load(open(specimen_json_path))
            key = specimen_dict["provenance"]["document_id"]
            specimenkeys.append(key)

            genus_species_list = specimen_dict.get("genus_species", [])
            if genus_species_list:
                genus_species = genus_species_list[0].get("ontology", "").upper()
            else:
                genus_species = ""
            genera.add(genus_species)

            disease_list = specimen_dict.get("diseases", [])
            if disease_list:
                disease = disease_list[0].get("ontology").upper()
            else:
                disease = ""

            diseases.add(disease)

        for donor_json_path in donor_json_paths:
            donor_dict = json.load(open(donor_json_path))

            ethnicity_list = donor_dict.get("human_specific", {}).get("ethnicity", [])
            if ethnicity_list:
                ethnicity = ethnicity_list[0].get("ontology").upper()
            else:
                ethnicity = ""

            dev_stage = donor_dict.get("development_stage", {}).get("ontology", "").upper()

            ethnicities.add(ethnicity)
            dev_stages.add(dev_stage)

        # Some cells have multiple donors/specimens. Only record the donor information if
        # it's the same across all of them.
        output = {
            "key": sorted(specimenkeys)[0],
            "genus_species": next(iter(genera)) if len(genera) == 1 else "",
            "ethnicity": next(iter(ethnicities)) if len(ethnicities) == 1 else "",
            "disease": next(iter(diseases)) if len(diseases) == 1 else "",
            "development_stage": next(iter(dev_stages)) if len(dev_stages) == 1 else ""
        }

        output.update(self.parse_organ(bundle_path))

        return output

    def parse_organ(self, bundle_path):
        """Get the organ and organ_parts. Have to look at the whole bundle to see if
        it's an organoid.
        """

        organoid_json_paths = glob.glob(os.path.join(bundle_path, "organoid_*.json"))

        if organoid_json_paths:
            model_organs = set()
            for organoid_json_path in organoid_json_paths:
                organoid_json = json.load(open(organoid_json_path))
                model_organ = lookup(organoid_json, "model_organ", "model_for_organ")["ontology"]
                model_organs.add(model_organ)

            if len(model_organs) == 1:
                return {"organ": next(iter(model_organs)) + " (organoid)",
                        "organ_parts": next(iter(model_organs)) + " (organoid)"}
            return {"organ": "", "organ_parts": ""}

        # Now see if it's a cell line with a selected cell type
        cell_line_json_paths = glob.glob(os.path.join(bundle_path, "cell_line_*.json"))
        if cell_line_json_paths:
            cell_suspension_path = os.path.join(bundle_path, "cell_suspension_0.json")
            cell_suspension_json = json.load(open(cell_suspension_path))
            selected_cell_type = lookup(cell_suspension_json,
                                        "selected_cell_types",
                                        "selected_cell_type")[0]["ontology"]
            return {"organ": selected_cell_type + " (cell line)",
                    "organ_parts": selected_cell_type + " (cell line)"}

        specimen_json_paths = glob.glob(os.path.join(bundle_path, "specimen_from_organism_*.json"))
        organs = set()
        organ_parts = set()
        for specimen_json_path in specimen_json_paths:
            specimen_json = json.load(open(specimen_json_path))
            organ = specimen_json.get("organ", {}).get("ontology", "")
            organ_part_list = specimen_json.get("organ_parts", [])
            if organ_part_list:
                organ_part = sorted(organ_part_list)[0].get("ontology", "")
            else:
                organ_part = ""
            organs.add(organ)
            organ_parts.add(organ_part)

        return {
            "organ": next(iter(organs)) if len(organs) == 1 else "",
            "organ_parts": next(iter(organ_parts)) if len(organ_parts) == 1 else ""
        }

    def parse_library_json(self, library_json_path):
        library_dict = json.load(open(library_json_path))
        key = library_dict["provenance"]["document_id"]

        input_nucleic_acid = library_dict.get(
            "input_nucleic_acid_molecule", {}).get("ontology", "").upper()
        construction_method = lookup(library_dict,
                                     "library_construction_method",
                                     "library_construction_approach").get("ontology", "").upper()
        end_bias = library_dict.get("end_bias", "")
        strand = library_dict.get("strand", "")

        return {
            "key": key,
            "input_nucleic_acid": input_nucleic_acid,
            "construction_method": construction_method,
            "end_bias": end_bias,
            "strand": strand
        }

import glob
import json
import os
import pathlib
import re
import sys
import typing
from threading import Lock

import requests

from matrix.common.aws.redshift_handler import TableName
from . import MetadataToPsvTransformer


class SpecimenLibraryTransformer(MetadataToPsvTransformer):
    """
    Reads specimen and library preparation metadata and writes out rows for
    specimen and library_preparation tables in PSV format.
    """
    WRITE_LOCK = Lock()

    @staticmethod
    def _is_consistent(values):
        """Return true if all the values are equal."""
        return all(el == values[0] for el in values)

    @staticmethod
    def _join(values):
        """When there are multiple value, for example multiple organ_parts, join them
        by semicolon.
        """
        return ';'.join([str(v) for v in values])

    def __init__(self, staging_dir):
        super(SpecimenLibraryTransformer, self).__init__(staging_dir)

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with SpecimenLibraryTransformer.WRITE_LOCK:
            super(SpecimenLibraryTransformer, self)._write_rows_to_psvs(*args)

    def _parse_from_metadatas(self, search_dir: str):
        p = pathlib.Path(search_dir)

        # There's a donor in every bundle we want to etl from
        bundle_paths = set(os.path.dirname(json_path) for json_path
                           in p.glob("**/donor_organism_*.json"))

        donor_infos = set()
        specimen_infos = set()
        cs_infos = set()
        library_infos = set()

        for bundle_path in bundle_paths:

            # Get the entity keys for this bundle, This needs to handle the
            # case where there are multiple entities, like multiple donors.
            donorkey = self.get_entity_key(bundle_path, "donor_organism_*.json")
            specimenkey = self.get_entity_key(bundle_path, "specimen_from_organism_*.json")
            cskey = self.get_entity_key(bundle_path, "cell_suspension_*.json")
            librarykey = self.get_entity_key(bundle_path, "library_preparation_protocol_*.json")

            # Parse the relevant jsons
            donor_info = self.parse_all_entity_jsons(
                bundle_path, "donor_organism_*.json", self.parse_donor_json)
            donor_info["donorkey"] = donorkey

            specimen_info = self.parse_all_entity_jsons(
                bundle_path, "specimen_from_organism_*.json", self.parse_specimen_json)
            specimen_info["specimenkey"] = specimenkey
            specimen_info["donorkey"] = donorkey

            cs_info = self.parse_all_entity_jsons(
                bundle_path, "cell_suspension_*.json", self.parse_cs_json)
            cs_info["specimenkey"] = specimenkey
            cs_info["cellsuspensionkey"] = cskey

            library_info = self.parse_all_entity_jsons(
                bundle_path, "library_preparation_protocol_*.json", self.parse_library_json)
            library_info["librarykey"] = librarykey

            # Handle special organ situations (cell lines and organoids)
            special_organ = self.parse_special_organ(bundle_path)

            # If we do have a special organ situation, put that information in
            # the cell_suspension
            if special_organ:
                cs_info["organ_ontology"] = special_organ["organ_ontology"]
                cs_info["organ_parts_ontologies"] = (special_organ["organ_parts_ontology"],)
            else:
                cs_info["organ_ontology"] = specimen_info["organ_ontology"]
                cs_info["organ_parts_ontologies"] = specimen_info["organ_parts_ontologies"]

            donor_infos.add(tuple(donor_info.items()))
            specimen_infos.add(tuple(specimen_info.items()))
            cs_infos.add(tuple(cs_info.items()))
            library_infos.add(tuple(library_info.items()))

        odict = self.create_ontology_resolver(specimen_infos, library_infos, donor_infos, cs_infos)

        donor_data = []
        for donor_info in (dict(d) for d in donor_infos):
            ethnicity_labels = [odict.get(e, "") for e in donor_info["ethnicity_ontologies"]]
            disease_labels = [odict.get(d, "") for d in donor_info["diseases_ontologies"]]
            dev_stage_label = odict.get(donor_info["development_stage_ontology"], "")

            donor_data.append(
                self._generate_psv_row(
                    donor_info["donorkey"],
                    self._join(donor_info["ethnicity_ontologies"]),
                    self._join(ethnicity_labels),
                    self._join(donor_info["diseases_ontologies"]),
                    self._join(disease_labels),
                    donor_info["development_stage_ontology"],
                    dev_stage_label,
                    donor_info["sex"],
                    donor_info["is_living"]))

        specimen_data = []
        for specimen_info in (dict(s) for s in specimen_infos):
            organ_label = odict.get(specimen_info["organ_ontology"], "")
            organ_parts_labels = [odict.get(o, "") for o in specimen_info["organ_parts_ontologies"]]
            disease_labels = [odict.get(d, "") for d in specimen_info["diseases_ontologies"]]
            specimen_data.append(
                self._generate_psv_row(
                    specimen_info["specimenkey"],
                    specimen_info["donorkey"],
                    specimen_info["organ_ontology"],
                    organ_label,
                    self._join(specimen_info["organ_parts_ontologies"]),
                    self._join(organ_parts_labels),
                    self._join(specimen_info["diseases_ontologies"]),
                    self._join(disease_labels)))

        cs_data = []
        for cs_info in (dict(c) for c in cs_infos):
            organ_label = odict.get(cs_info["organ_ontology"], "")
            organ_parts_labels = [odict.get(o, "") for o in cs_info["organ_parts_ontologies"]]
            genus_species_labels = [odict.get(g, "") for g in cs_info["genus_species_ontologies"]]
            cs_data.append(
                self._generate_psv_row(
                    cs_info["cellsuspensionkey"],
                    cs_info["specimenkey"],
                    cs_info["organ_ontology"],
                    organ_label,
                    self._join(cs_info["organ_parts_ontologies"]),
                    self._join(organ_parts_labels),
                    self._join(cs_info["genus_species_ontologies"]),
                    self._join(genus_species_labels)))

        library_data = []
        for library_info in (dict(l) for l in library_infos):
            library_data.append(
                self._generate_psv_row(library_info['librarykey'],
                                       library_info['input_nucleic_acid_ontology'],
                                       odict.get(library_info["input_nucleic_acid_ontology"], ""),
                                       library_info['construction_method_ontology'],
                                       odict.get(library_info['construction_method_ontology'], ""),
                                       library_info['end_bias'],
                                       library_info['strand']))

        return ((TableName.SPECIMEN, specimen_data), (TableName.LIBRARY_PREPARATION, library_data),
                (TableName.DONOR, donor_data), (TableName.CELL_SUSPENSION, cs_data))

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
            try:
                # Well this isn't right, but it's present in the metadata
                ontology, term_id = term.split("_")
            except ValueError:
                print(f"Term is malformed: {term}!", file=sys.stderr)
                return None

        ontology = special_case.get(ontology, ontology)

        # Match a trailing comment like "... (organoid)" or "... (cell line)". If there
        # is a match, strip it off and hold on to it.
        special_match = re.match(r'(.*)(\ \(.*\))', term_id)
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

    def create_ontology_resolver(self, *metadata_tuple_sets):
        """Look through all the gathered metadata and create a dict that maps
        from ontology term to ontology label.
        """

        candidate_terms = set()
        for metadata_tuple_set in metadata_tuple_sets:
            for metadata_tuple in metadata_tuple_set:

                for key, value in metadata_tuple:
                    if "ontolog" in key:
                        if isinstance(value, tuple):
                            candidate_terms.update(value)
                        else:
                            candidate_terms.add(value)

        resolver = {}
        for candidate_term in candidate_terms:
            label = self.resolve_ontology_ols(candidate_term)
            if label:
                resolver[candidate_term] = label

        return resolver

    def get_entity_key(self, bundle_path, entity_json_glob):
        """Given an entity glob pattern for an entity type, find a key
        that identifies that entity in the given bundle. This has to handle
        multiple entitities by sorting keys and choosing the first one.
        """
        json_paths = glob.glob(
            os.path.join(bundle_path, entity_json_glob))

        keys = []
        for json_path in json_paths:
            entity_dict = json.load(open(json_path))
            key = entity_dict["provenance"]["document_id"]
            keys.append(key)

        return sorted(keys)[0]

    def parse_donor_json(self, donor_json_path):
        """Parse a donor_organism json"""

        donor_dict = json.load(open(donor_json_path))

        ethnicity_list = donor_dict.get("human_specific", {}).get("ethnicity", [])
        ethnicity_ontologies = [e.get("ontology").upper() for e in ethnicity_list]

        disease_list = donor_dict.get("diseases", [])
        diseases_ontologies = [e.get("ontology").upper() for e in disease_list]

        dev_stage = donor_dict.get("development_stage", {}).get("ontology", "").upper()

        sex = donor_dict.get("sex", "")

        is_living = donor_dict.get("is_living", "")

        return {
            "ethnicity_ontologies": tuple(ethnicity_ontologies),
            "diseases_ontologies": tuple(diseases_ontologies),
            "development_stage_ontology": dev_stage,
            "sex": sex,
            "is_living": is_living
        }

    def parse_specimen_json(self, specimen_json_path):
        """Parse a specimen_from_organism json"""

        specimen_dict = json.load(open(specimen_json_path))

        genus_species_list = specimen_dict.get("genus_species", [])
        genus_species_ontologies = [e.get("ontology").upper() for e in genus_species_list]

        organ = specimen_dict.get("organ", {}).get("ontology", "")

        organ_parts_list = specimen_dict.get("organ_parts", [])
        organ_parts_ontologies = [e.get("ontology").upper() for e in organ_parts_list]

        diseases_list = specimen_dict.get("diseases", [])
        diseases_ontologies = [e.get("ontology").upper() for e in diseases_list]

        return {
            "genus_species_ontologies": tuple(genus_species_ontologies),
            "organ_ontology": organ,
            "organ_parts_ontologies": tuple(organ_parts_ontologies),
            "diseases_ontologies": tuple(diseases_ontologies)
        }

    def parse_cs_json(self, cs_json_path):
        """Parse a cell_suspension json. The derived organ information is calculated
        elsewhere since it requires looking at the whole bundle.
        """

        cs_dict = json.load(open(cs_json_path))
        genus_species_list = cs_dict.get("genus_species", [])
        genus_species_ontologies = [e.get("ontology").upper() for e in genus_species_list]

        return {
            "genus_species_ontologies": tuple(genus_species_ontologies)
        }

    def parse_all_entity_jsons(self, bundle_path, entity_json_glob, entity_parser):
        """Given a glob that identifies an entity, like "donor_organism_*.json", parse all
        those entity jsons. Then handle the case where there are multiple entities in a
        single bundle, and return a dict of the parsed values.
        """

        json_paths = glob.glob(
            os.path.join(bundle_path, entity_json_glob))

        entity_info = {}

        for json_path in json_paths:
            parsed_dict = entity_parser(json_path)
            for key in parsed_dict:
                entity_info.setdefault(key, []).append(parsed_dict[key])

        output = {}
        for key, value in entity_info.items():
            if self._is_consistent(value):
                output[key] = value[0]
            else:
                output[key] = ("",) if isinstance(value, tuple) else ""
        return output

    def parse_special_organ(self, bundle_path):
        """Get the organ and organ_parts for special situations. Have to look at the whole bundle
        to see if it's a cell line or organoid.

        If there is no special situation, return None.
        """

        organoid_json_paths = glob.glob(os.path.join(bundle_path, "organoid_*.json"))

        if organoid_json_paths:
            model_organ_ontologies = set()
            model_organ_part_ontologies = set()
            for organoid_json_path in organoid_json_paths:
                organoid_json = json.load(open(organoid_json_path))

                model_organ_ontology = organoid_json.get("model_organ", {})["ontology"].upper()
                model_organ_part_ontology = organoid_json.get("model_organ_part", {}).get("ontology", "").upper()

                model_organ_ontologies.add(model_organ_ontology)
                if model_organ_part_ontology:
                    model_organ_part_ontologies.add(model_organ_part_ontology)

            if len(model_organ_ontologies) == 1:
                organ_ontology = next(iter(model_organ_ontologies)) + " (organoid)"
            else:
                organ_ontology = ""
            if len(model_organ_part_ontologies) == 1:
                organ_part_ontology = next(iter(model_organ_part_ontologies)) + " (organoid)"
            else:
                organ_part_ontology = ""
            return {"organ_ontology": organ_ontology, "organ_parts_ontology": organ_part_ontology}

        # Now see if it's a cell line with a selected cell type
        cell_line_json_paths = glob.glob(os.path.join(bundle_path, "cell_line_*.json"))
        if cell_line_json_paths:
            model_organ_ontologies = set()
            model_organ_part_ontologies = set()
            for cell_line_json_path in cell_line_json_paths:
                cell_line_json = json.load(open(cell_line_json_path))

                model_organ_ontology = cell_line_json.get("model_organ", {})["ontology"].upper()
                model_organ_part_ontology = cell_line_json.get("tissue", {}).get("ontology", "").upper()

                model_organ_ontologies.add(model_organ_ontology)
                if model_organ_part_ontology:
                    model_organ_part_ontologies.add(model_organ_part_ontology)

            if len(model_organ_ontologies) == 1:
                organ_ontology = next(iter(model_organ_ontologies)) + " (cell line)"
            else:
                organ_ontology = ""
            if len(model_organ_part_ontologies) == 1:
                organ_part_ontology = next(iter(model_organ_part_ontologies)) + " (cell line)"
            else:
                organ_part_ontology = ""
            return {"organ_ontology": organ_ontology, "organ_parts_ontology": organ_part_ontology}

    def parse_library_json(self, library_json_path):
        """Parse a library_preparation_protocol json file."""
        library_dict = json.load(open(library_json_path))

        input_nucleic_acid = library_dict["input_nucleic_acid_molecule"]["ontology"].upper()
        construction_method = library_dict["library_construction_method"]["ontology"].upper()
        end_bias = library_dict.get("end_bias", "")
        strand = library_dict.get("strand", "")

        return {
            "input_nucleic_acid_ontology": input_nucleic_acid,
            "construction_method_ontology": construction_method,
            "end_bias": end_bias,
            "strand": strand
        }

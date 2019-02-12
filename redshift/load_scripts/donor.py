"""
CREATE TABLE donor_organism (
    donorkey           VARCHAR(40) NOT NULL,
    genus_species_ontology      VARCHAR(40),
    genus_species_label         VARCHAR(40),
    ethnicity_ontology          VARCHAR(40),
    ethnicity_label             VARCHAR(40),
    disease_ontology            VARCHAR(40),
    disease_label               VARCHAR(40),
    development_stage_ontology  VARCHAR(40),
    development_stage_label     VARCHAR(40),
    organ_ontology              VARCHAR(40),
    organ_label                 VARCHAR(40),
    organ_part_ontology         VARCHAR(40),
    organ_part_label            VARCHAR(40),
    PRIMARY KEY(donorkey))
    DISTSTYLE ALL
    SORTKEY(donorkey)
;

CREATE TABLE library_preparation (
    librarykey                       VARCHAR(40) NOT NULL,
    input_nucleic_acid_ontology      VARCHAR(40),
    input_nucleic_acid_label         VARCHAR(40),
    construction_approach_ontology   VARCHAR(40),
    construction_approach_label      VARCHAR(40),
    end_bias                         VARCHAR(20),
    strand                           VARCHAR(20),
    PRIMARY KEY(librarykey))
    DISTSTYLE ALL
    SORTKEY(librarykey)
;
"""

import glob
import json
import os
import pathlib

import requests

def resolve_ontology_ols(term):
    """Get the ontology label for a term using the OLS API."""

    special_case = {
        "NCBITAXON": "NCBITaxon"
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
    is_organoid = term_id.endswith(" (organoid)")
    term_id = term_id.rstrip(" (organoid)")

    label = None
    for uri_template in uri_templates:
        uri = uri_template.format(ontology=ontology, lontology=ontology.lower(),
                                  term_id=term_id)
        print("uri:", uri)
        resp = requests.get(uri)
        if "label" in resp.json():
            label = resp.json()["label"]
            if is_organoid:
                label += " (organoid)"
            break
    return label

def create_ontology_resolver(*metadata_dict_lists):
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
        label = resolve_ontology_ols(candidate_term)
        if label:
            resolver[candidate_term] = label

    return resolver


def parse_donor_json(bundle_path):
    donor_json_paths = glob.glob(
        os.path.join(bundle_path, "donor_organism_*.json"))

    genera = set()
    ethnicities = set()
    diseases = set()
    dev_stages = set()
    donorkeys = []
    
    for donor_json_path in donor_json_paths:
        donor_dict = json.load(open(donor_json_path))
        key = donor_dict["provenance"]["document_id"]

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

        dev_stage = donor_dict.get("development_stage", {}).get("ontology", "").upper()

        donorkeys.append(key)
        genera.add(genus_species)
        ethnicities.add(ethnicity)
        diseases.add(disease)
        dev_stages.add(dev_stage)
    
    # Some cells have multiple donors. Only record the donor information if
    # it's the same across all of them.
    output = {
        "key": sorted(donorkeys)[0],
        "genus_species": next(iter(genera)) if len(genera) == 1 else "",
        "ethnicity": next(iter(ethnicities)) if len(ethnicities) == 1 else "",
        "disease": next(iter(diseases)) if len(diseases) == 1 else "",
        "development_stage": next(iter(dev_stages)) if len(dev_stages) == 1 else ""
    }

    output.update(parse_organ(bundle_path))

    return output

def parse_organ(bundle_path):
    """Get the organ and organ_part. Have to look at the whole bundle to see if
    it's an organoid.
    """

    organoid_json_paths = glob.glob(os.path.join(bundle_path, "organoid_*.json"))

    if organoid_json_paths:
        model_organs = set()
        for organoid_json_path in organoid_json_paths:
            organoid_json = json.load(open(organoid_json_path))
            model_organ = organoid_json["model_for_organ"]["ontology"]
            model_organs.add(model_organ)

        if len(model_organs) == 1:
            return {"organ": next(iter(model_organs)) + " (organoid)",
                    "organ_part": next(iter(model_organs)) + " (organoid)"}
        return {"organ": "", "organ_part": ""}

    specimen_json_paths = glob.glob(os.path.join(bundle_path, "specimen_from_organism_*.json"))
    organs = set()
    organ_parts = set()
    for specimen_json_path in specimen_json_paths:
        specimen_json = json.load(open(specimen_json_path))
        organ = specimen_json.get("organ", {}).get("ontology", "")
        organ_part = specimen_json.get("organ_part", {}).get("ontology", "")
        organs.add(organ)
        organ_parts.add(organ_part)

    return {
        "organ": next(iter(organs)) if len(organs) == 1 else "",
        "organ_part": next(iter(organ_parts)) if len(organ_parts) == 1 else ""
    }


def parse_library_json(library_json_path):
    library_dict = json.load(open(library_json_path))
    key = library_dict["provenance"]["document_id"]

    input_nucleic_acid = library_dict.get(
        "input_nucleic_acid_molecule", {}).get("ontology", "").upper()
    construction_approach = library_dict.get(
        "library_construction_approach", {}).get("ontology", "").upper()
    end_bias = library_dict.get("end_bias", "")
    strand = library_dict.get("strand", "")

    return {
        "key": key,
        "input_nucleic_acid": input_nucleic_acid,
        "construction_approach": construction_approach,
        "end_bias": end_bias,
        "strand": strand
    }

def main():
    p = pathlib.Path(".")

    donor_infos = []
    for donor_json_path in p.glob("**/donor_organism_*.json"):
        donor_info = parse_donor_json(os.path.dirname(donor_json_path))
        if donor_info:
            donor_infos.append(donor_info)

    library_infos = []
    for library_json_path in p.glob("**/library_preparation_protocol_*.json"):
        library_info = parse_library_json(library_json_path)
        if library_info:
            library_infos.append(library_info)

    odict = create_ontology_resolver(donor_infos, library_infos)

    donor_data = set()
    for donor_info in donor_infos:
        donor_data.add(
            '|'.join([donor_info['key'],
                      donor_info['genus_species'], odict.get(donor_info['genus_species'], ""),
                      donor_info['ethnicity'], odict.get(donor_info["ethnicity"], ""),
                      donor_info['disease'], odict.get(donor_info["disease"], ""),
                      donor_info['development_stage'], odict.get(donor_info["development_stage"], ""),
                      donor_info["organ"], odict.get(donor_info["organ"], ""),
                      donor_info["organ_part"], odict.get(donor_info["organ_part"], "")]))
    with open("donor.data", "w") as donor_data_file:
        for donor_line in donor_data:
            donor_data_file.write(donor_line + '\n')

    library_data = set()
    for library_info in library_infos:
        library_data.add(
            '|'.join([library_info['key'],
                      library_info['input_nucleic_acid'],
                      odict.get(library_info["input_nucleic_acid"], ""),
                      library_info['construction_approach'],
                      odict.get(library_info['construction_approach'], ""),
                      library_info['end_bias'],
                      library_info['strand']]))
    with open("library.data", "w") as library_data_file:
        for library_line in library_data:
            library_data_file.write(library_line + '\n')
main()

"""
CREATE TABLE donor_organism (
    donor_key                VARCHAR(40) NOT NULL SORTKEY,
    donor_genus_species      VARCHAR(40),
    donor_ethnicity          VARCHAR(40),
    donor_disease            VARCHAR(40),
    donor_development_stage  VARCHAR(40)
) DISTSTYLE ALL;

CREATE TABLE library_preparation (
    library_key                    VARCHAR(40) NOT NULL SORTKEY,
    library_input_nucleic_acid     VARCHAR(40),
    library_construction_approach  VARCHAR(40),
    library_end_bias               VARCHAR(20),
    library_strand                 VARCHAR(20)
) DISTSTYLE ALL;
"""

import json
import pathlib

def parse_donor_json(donor_json_path):
    donor_dict = json.load(open(donor_json_path))
    key = donor_dict["provenance"]["document_id"]

    if "DEMO" in donor_dict["biomaterial_core"].get("biomaterial_id", ""):
        return {}

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

    return {
        "key": key,
        "genus_species": genus_species,
        "ethnicity": ethnicity,
        "disease": disease,
        "development_stage": development_stage
    }

def parse_library_json(library_json_path):
    library_dict = json.load(open(library_json_path))
    key = library_dict["provenance"]["document_id"]

    input_nucleic_acid = library_dict.get("input_nucleic_acid_molecule", {}).get("ontology", "").upper()
    construction_approach = library_dict.get("library_construction_approach", {}).get("ontology", "").upper()
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
        donor_info = parse_donor_json(donor_json_path)
        if donor_info:
            donor_infos.append(donor_info)

    donor_data = set()
    for donor_info in donor_infos:
        donor_data.add(
            '|'.join([donor_info['key'], donor_info['genus_species'],
                      donor_info['ethnicity'], donor_info['disease'], donor_info['development_stage']]))
    with open("donor.data", "w") as donor_data_file:
        for donor_line in donor_data:
            donor_data_file.write(donor_line + '\n')

    library_infos = []
    for library_json_path in p.glob("**/library_preparation_protocol_*.json"):
        library_info = parse_library_json(library_json_path)
        if library_info:
            library_infos.append(library_info)

    library_data = set()
    for library_info in library_infos:
        library_data.add(
            '|'.join([library_info['key'], library_info['input_nucleic_acid'],
                      library_info['construction_approach'], library_info['end_bias'],
                      library_info['strand']]))
    with open("library.data", "w") as library_data_file:
        for library_line in library_data:
            library_data_file.write(library_line + '\n')
main()

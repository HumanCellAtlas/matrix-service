"""
CREATE TABLE expression (
    expr_projectkey       VARCHAR(60) NOT NULL SORTKEY,
    expr_cellkey          VARCHAR(60) NOT NULL DISTKEY,
    expr_donorkey         VARCHAR(60),
    expr_librarykey       VARCHAR(60),
    expr_featurekey       VARCHAR(20) NOT NULL,
    expr_exprtype         VARCHAR(10) NOT NULL,
    expr_value            NUMERIC(12, 2) NOT NULL
);

CREATE TABLE cell (
    cell_key              VARCHAR(60) NOT NULL SORTKEY,
    cell_barcode          VARCHAR(32),
    cell_is_lucky         BOOLEAN
);
"""

import argparse
import csv
import glob
import hashlib
import json
import os
import pathlib

import scipy.io


def parse_keys(bundle_path):
    p = pathlib.Path(bundle_path)

    project_path = p.joinpath("project_0.json")
    project_key = json.load(open(project_path))["provenance"]["document_id"]

    donor_paths = list(p.glob("donor_organism_*.json"))
    if len(donor_paths) > 1:
        donor_key = ""
    else:
        donor_key = json.load(open(donor_paths[0]))["provenance"]["document_id"]

    library_paths = list(p.glob("library_preparation_protocol_*.json"))
    if len(library_paths) > 1:
        library_key = ""
    else:
        library_key = json.load(open(library_paths[0]))["provenance"]["document_id"]

    return {
        "project_key": project_key,
        "donor_key": donor_key,
        "library_key": library_key
    }


def parse_10x_bundle(bundle_path):

    keys = parse_keys(bundle_path)

    matrix = scipy.io.mmread(os.path.join(bundle_path, "matrix.mtx"))
    genes = [g.split("\t")[0].split(".", 1)[0] for g in
             open(os.path.join(bundle_path, "genes.tsv")).readlines()]
    barcodes = [b.strip() for b in open(os.path.join(bundle_path, "barcodes.tsv")).readlines()]
    cell_suspension_id = json.load(open(
        os.path.join(bundle_path, "cell_suspension_0.json")))["provenance"]["document_id"]
    sequence_file_path = list(pathlib.Path(bundle_path).glob("sequence_file_*.json"))[0]
    lane_index = json.load(open(sequence_file_path))["lane_index"]

    # columns are cells, rows are genes
    expression_lines = []
    cell_lines = set()
    for i, j, v in zip(matrix.row, matrix.col, matrix.data):
        barcode = barcodes[j]
        gene = genes[i]

        # Just make up a cell id
        h = hashlib.md5()
        h.update(keys["project_key"].encode())
        h.update(keys["donor_key"].encode())
        h.update(cell_suspension_id.encode())
        h.update(barcode.encode())
        h.update(str(lane_index).encode())
        cell_key = h.hexdigest()

        # decide if this is a lucky cell
        is_lucky = str(ord(cell_key[-1]) % 5 == 0)
        expression_line = '|'.join(
            [keys["project_key"],
             cell_key,
             keys["donor_key"],
             keys["library_key"],
             gene,
             "Count",
             str(v)]) + '\n'
        cell_line = '|'.join(
            [cell_key,
             barcode,
             is_lucky]) + '\n'
        cell_lines.add(cell_line)

        expression_lines.append(expression_line)
        cell_lines.add(cell_line)


    return cell_lines, expression_lines

def parse_ss2_bundle(bundle_path):

    # Get the keys associated with this cell, except for cellkey
    keys = parse_keys(bundle_path)
    cell_key = json.load(open(
        os.path.join(bundle_path, "cell_suspension_0.json")))["provenance"]["document_id"]

    # Read in isoform and gene expression values
    isoforms_path = glob.glob(os.path.join(bundle_path, "*.isoforms.results"))[0]
    isoforms_values = {}
    with open(isoforms_path) as iso_file:
        reader = csv.DictReader(iso_file, delimiter='\t')
        for row in reader:
            isoforms_values[row["transcript_id"].split(".")[0]] = {
                "TPM": float(row["TPM"]), "Count": float(row["expected_count"])}

    genes_path = glob.glob(os.path.join(bundle_path, "*.genes.results"))[0]
    genes_values = {}
    with open(genes_path) as genes_file:
        reader = csv.DictReader(genes_file, delimiter='\t')
        for row in reader:
            genes_values[row["gene_id"].split(".")[0]] = {
                "TPM": float(row["TPM"]), "Count": float(row["expected_count"])}


    # Now prepare the for the redshift table
    # cell is easy because there's only one
    is_lucky = str(ord(cell_key[-1]) % 5 == 0)
    cell_lines = ['|'.join([cell_key, "", str(is_lucky)]) + '\n']

    expression_lines = []
    for transcript_id, expr_values in isoforms_values.items():
        if expr_values["Count"] == 0:
            continue
        for expr_type in ["TPM", "Count"]:
            expression_line = '|'.join(
                [keys["project_key"],
                 cell_key,
                 keys["donor_key"],
                 keys["library_key"],
                 transcript_id,
                 expr_type,
                 str(expr_values[expr_type])]) + '\n'
            expression_lines.append(expression_line)
    for gene_id, expr_values in genes_values.items():
        if expr_values["Count"] == 0:
            continue
        for expr_type in ["TPM", "Count"]:
            expression_line = '|'.join(
                [keys["project_key"],
                 cell_key,
                 keys["donor_key"],
                 keys["library_key"],
                 gene_id,
                 expr_type,
                 str(expr_values[expr_type])]) + '\n'
            expression_lines.append(expression_line)

    return cell_lines, expression_lines

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle_dir")
    args = parser.parse_args()

    if os.path.isfile(os.path.join(args.bundle_dir, "matrix.mtx")):
        cell_lines, expression_lines = parse_10x_bundle(args.bundle_dir)
    else:
        cell_lines, expression_lines = parse_ss2_bundle(args.bundle_dir)

    with open("cell.data", "a") as cell_file:
        print("Writing", cell_lines)
        cell_file.writelines(cell_lines)

    with open("expression.data", "a") as expression_file:
        expression_file.writelines(expression_lines)

main()

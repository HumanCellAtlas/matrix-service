"""
CREATE TABLE cell (
    cellkey          VARCHAR(60) NOT NULL,
    projectkey       VARCHAR(60) NOT NULL,
    donorkey         VARCHAR(60) NOT NULL,
    librarykey       VARCHAR(60) NOT NULL,
    analysiskey      VARCHAR(60) NOT NULL,
    barcode          VARCHAR(32),
    genes_detected   INTEGER,
    PRIMARY KEY(cellkey),
    FOREIGN KEY(projectkey) REFERENCES project(projectkey),
    FOREIGN KEY(donorkey) REFERENCES donor_organism(donorkey),
    FOREIGN KEY(librarykey) REFERENCES library_preparation(librarykey),
    FOREIGN KEY(analysiskey) REFERENCES analysis(analysiskey))
    DISTKEY(cellkey)
    SORTKEY(cellkey, projectkey)
;

CREATE TABLE expression (
    cellkey          VARCHAR(60) NOT NULL,
    featurekey       VARCHAR(20) NOT NULL,
    exprtype         VARCHAR(10) NOT NULL,
    exrpvalue        REAL NOT NULL,
    FOREIGN KEY(cellkey) REFERENCES cell(cellkey))
    DISTKEY(cellkey)
    COMPOUND SORTKEY(cellkey, featurekey)
;
"""

import argparse
import csv
import glob
import gzip
import hashlib
import json
import os
import pathlib

import scipy.io


def parse_keys(bundle_path):
    p = pathlib.Path(bundle_path)

    project_path = p.joinpath("project_0.json")
    project_key = json.load(open(project_path))["provenance"]["document_id"]

    ap_path = p.joinpath("analysis_protocol_0.json")
    ap_key = json.load(open(ap_path))["provenance"]["document_id"]

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
        "library_key": library_key,
        "analysis_key": ap_key
    }


def parse_10x_bundle(bundle_path):

    keys = parse_keys(bundle_path)

    matrix = scipy.io.mmread(os.path.join(bundle_path, "matrix.mtx"))
    genes = [g.split("\t")[0].split(".", 1)[0] for g in
             open(os.path.join(bundle_path, "genes.tsv")).readlines()]
    barcodes = [b.strip() for b in open(os.path.join(bundle_path, "barcodes.tsv")).readlines()]
    genes_detected = (matrix != 0).sum(0)
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
        gene_count = genes_detected.item((0, j))

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
            [cell_key,
             gene,
             "Count",
             str(v)]) + '\n'
        cell_line = '|'.join(
            [cell_key,
             keys["project_key"],
             keys["donor_key"],
             keys["library_key"],
             keys["analysis_key"],
             barcode,
             str(gene_count)]) + '\n'
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

    genes_detected = sum((1 for k in genes_values.values() if k["Count"] > 0))
    # Now prepare the for the redshift table
    # cell is easy because there's only one
    is_lucky = str(ord(cell_key[-1]) % 5 == 0)
    cell_lines = ['|'.join([
        cell_key,
        keys["project_key"],
        keys["donor_key"],
        keys["library_key"],
        keys["analysis_key"],
        "",
        str(genes_detected)]) + '\n']

    expression_lines = []
    for transcript_id, expr_values in isoforms_values.items():
        if expr_values["Count"] == 0:
            continue
        for expr_type in ["TPM", "Count"]:
            expression_line = '|'.join(
                [cell_key,
                 transcript_id,
                 expr_type,
                 str(expr_values[expr_type])]) + '\n'
            expression_lines.append(expression_line)
    for gene_id, expr_values in genes_values.items():
        if expr_values["Count"] == 0:
            continue
        for expr_type in ["TPM", "Count"]:
            expression_line = '|'.join(
                [cell_key,
                 gene_id,
                 expr_type,
                 str(expr_values[expr_type])]) + '\n'
            expression_lines.append(expression_line)

    return cell_lines, expression_lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle_dir")
    parser.add_argument("output_dir")
    args = parser.parse_args()

    if os.path.isfile(os.path.join(args.bundle_dir, "matrix.mtx")):
        cell_lines, expression_lines = parse_10x_bundle(args.bundle_dir)
    else:
        cell_lines, expression_lines = parse_ss2_bundle(args.bundle_dir)

    cell_data_path = os.path.join(
        args.output_dir, os.path.split(os.path.normpath(args.bundle_dir))[-1] + '.cell.data.gz')
    with gzip.open(cell_data_path, "w") as cell_file:
        cell_file.writelines((c.encode() for c in cell_lines))

    expression_data_path = os.path.join(
        args.output_dir, os.path.split(os.path.normpath(args.bundle_dir))[-1] + '.expression.data.gz')
    with gzip.open(expression_data_path, "w") as expression_file:
        expression_file.writelines((e.encode() for e in expression_lines))

if __name__ == '__main__':
    main()

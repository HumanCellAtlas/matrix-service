import csv
import glob
import gzip
import hashlib
import json
import os
import pathlib
import typing
from threading import Lock

import scipy.io

from . import MetadataToPsvTransformer
from matrix.common.aws.redshift_handler import TableName


class CellExpressionTransformer(MetadataToPsvTransformer):
    """Reads SS2 and 10X bundles and writes out rows for expression and cell tables in PSV format."""
    WRITE_LOCK = Lock()

    def __init__(self, staging_dir):
        super(CellExpressionTransformer, self).__init__(staging_dir)

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with CellExpressionTransformer.WRITE_LOCK:
            for arg in args:
                table = arg[0]
                rows = arg[1]
                bundle_dir = arg[2]

                out_dir = os.path.join(self.output_dir, table.value)
                os.makedirs(out_dir, exist_ok=True)

                out_file_path = os.path.join(
                    self.output_dir,
                    table.value,
                    f"{os.path.split(os.path.normpath(bundle_dir))[-1]}.{table.value}.data.gz")
                with gzip.open(out_file_path, 'w') as out_file:
                    out_file.writelines((row.encode() for row in rows))

    def _parse_from_metadatas(self, bundle_dir):
        if os.path.isfile(os.path.join(bundle_dir, "matrix.mtx")):
            cell_lines, expression_lines = self._parse_10x_bundle(bundle_dir)
        else:
            cell_lines, expression_lines = self._parse_ss2_bundle(bundle_dir)

        return (TableName.CELL, cell_lines, bundle_dir), (TableName.EXPRESSION, expression_lines, bundle_dir)

    def _parse_ss2_bundle(self, bundle_dir):
        # Get the keys associated with this cell, except for cellkey
        keys = self._parse_keys(bundle_dir)
        cell_key = json.load(open(
            os.path.join(bundle_dir, "cell_suspension_0.json")))["provenance"]["document_id"]

        # Read in isoform and gene expression values
        isoforms_path = glob.glob(os.path.join(bundle_dir, "*.isoforms.results"))[0]
        isoforms_values = {}
        with open(isoforms_path) as iso_file:
            reader = csv.DictReader(iso_file, delimiter='\t')
            for row in reader:
                isoforms_values[row["transcript_id"].split(".")[0]] = {
                    "TPM": float(row["TPM"]), "Count": float(row["expected_count"])}

        genes_path = glob.glob(os.path.join(bundle_dir, "*.genes.results"))[0]
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

    def _parse_10x_bundle(self, bundle_dir):
        keys = self._parse_keys(bundle_dir)

        matrix = scipy.io.mmread(os.path.join(bundle_dir, "matrix.mtx"))
        genes = [g.split("\t")[0].split(".", 1)[0] for g in
                 open(os.path.join(bundle_dir, "genes.tsv")).readlines()]
        barcodes = [b.strip() for b in open(os.path.join(bundle_dir, "barcodes.tsv")).readlines()]
        genes_detected = (matrix != 0).sum(0)
        cell_suspension_id = json.load(open(
            os.path.join(bundle_dir, "cell_suspension_0.json")))["provenance"]["document_id"]
        sequence_file_path = list(pathlib.Path(bundle_dir).glob("sequence_file_*.json"))[0]
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

    def _parse_keys(self, bundle_dir):
        p = pathlib.Path(bundle_dir)

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
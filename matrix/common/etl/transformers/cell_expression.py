import csv
import glob
import gzip
import hashlib
import json
import os
import pathlib
import typing

import scipy.io

from . import MetadataToPsvTransformer
from matrix.common.aws.redshift_handler import TableName


class CellExpressionTransformer(MetadataToPsvTransformer):
    """Reads SS2 and 10X bundles and writes out rows for expression and cell tables in PSV format."""

    def __init__(self, staging_dir):
        super(CellExpressionTransformer, self).__init__(staging_dir)

    def _write_rows_to_psvs(self, *args: typing.Tuple):
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
            os.path.join(bundle_dir, "cell_suspension_0.json")))['provenance']['document_id']

        # Read in isoform and gene expression values
        isoforms_path = glob.glob(os.path.join(bundle_dir, "*.isoforms.results"))[0]
        isoforms_values = {}
        with open(isoforms_path) as iso_file:
            reader = csv.DictReader(iso_file, delimiter='\t')
            for row in reader:
                transcript_id = row['transcript_id'].split('.')[0]
                isoforms_values[transcript_id] = {
                    'TPM': float(row['TPM']) + isoforms_values.get(transcript_id, {}).get('TPM', 0),
                    'Count': float(row['expected_count']) + isoforms_values.get(transcript_id, {}).get('Count', 0)
                }

        genes_path = glob.glob(os.path.join(bundle_dir, "*.genes.results"))[0]
        genes_values = {}
        with open(genes_path) as genes_file:
            reader = csv.DictReader(genes_file, delimiter='\t')
            for row in reader:
                gene_id = row['gene_id'].split('.')[0]
                genes_values[gene_id] = {
                    'TPM': float(row['TPM']) + genes_values.get(gene_id, {}).get('TPM', 0),
                    'Count': float(row['expected_count']) + genes_values.get(gene_id, {}).get('Count', 0)
                }

        genes_detected = sum((1 for k in genes_values.values() if k["Count"] > 0))

        cell_lines = ['|'.join([
            cell_key,
            keys["project_key"],
            keys["specimen_key"],
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

        # columns are cells, rows are genes
        expression_lines = []
        cell_lines = set()
        cell_gene_counts = {}
        cell_to_barcode = {}

        for i, j, v in zip(matrix.row, matrix.col, matrix.data):
            barcode = barcodes[j]
            gene = genes[i]

            # Just make up a cell id
            h = hashlib.md5()
            bundle_uuid = pathlib.Path(bundle_dir).parts[-1]
            h.update(bundle_uuid.encode())
            h.update(barcode.encode())
            cell_key = h.hexdigest()

            cell_to_barcode[cell_key] = barcode

            if cell_key not in cell_gene_counts:
                cell_gene_counts[cell_key] = {}
            cell_gene_counts[cell_key][gene] = cell_gene_counts[cell_key].get(gene, 0) + v

        for cell_key, gene_count_dict in cell_gene_counts.items():

            for gene, count in gene_count_dict.items():
                expression_line = '|'.join(
                    [cell_key,
                     gene,
                     "Count",
                     str(count)]) + '\n'
                expression_lines.append(expression_line)

            gene_count = len(gene_count_dict)
            cell_line = '|'.join(
                [cell_key,
                 keys["project_key"],
                 keys["specimen_key"],
                 keys["library_key"],
                 keys["analysis_key"],
                 cell_to_barcode[cell_key],
                 str(gene_count)]) + '\n'
            cell_lines.add(cell_line)

        return cell_lines, expression_lines

    def _parse_keys(self, bundle_dir):
        p = pathlib.Path(bundle_dir)

        project_path = p.joinpath("project_0.json")
        project_key = json.load(open(project_path))["provenance"]["document_id"]

        ap_path = p.joinpath("analysis_protocol_0.json")
        ap_key = json.load(open(ap_path))["provenance"]["document_id"]

        specimen_paths = list(p.glob("specimen_from_organism_*.json"))
        specimen_keys = [json.load(open(p))['provenance']['document_id'] for p in specimen_paths]
        specimen_key = sorted(specimen_keys)[0]

        library_paths = list(p.glob("library_preparation_protocol_*.json"))
        library_keys = [json.load(open(p))['provenance']['document_id'] for p in library_paths]
        library_key = sorted(library_keys)[0]

        return {
            "project_key": project_key,
            "specimen_key": specimen_key,
            "library_key": library_key,
            "analysis_key": ap_key
        }

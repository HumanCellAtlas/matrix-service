import csv
import glob
import gzip
import hashlib
import json
import os
import pathlib
import typing

import numpy
import zarr

from . import MetadataToPsvTransformer
from matrix.common.aws.redshift_handler import TableName
from matrix.common.etl.dcp_zarr_store import DCPZarrStore
from matrix.common.exceptions import MatrixException
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class CellExpressionTransformer(MetadataToPsvTransformer):
    """Reads SS2 and 10X bundles and writes out rows for expression and cell tables in PSV format."""

    # The minimum UMI count in the emptydrops result required to include a
    # putative cell in the matrix service.
    emptydrops_min_count = 100

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

    def _parse_from_metadatas(self, bundle_dir, bundle_manifest_path):
        protocol_id = json.load(
            open(os.path.join(bundle_dir, "analysis_protocol_0.json")))['protocol_core']['protocol_id']
        if protocol_id.startswith("smartseq2"):
            cell_lines, expression_lines = self._parse_ss2_bundle(bundle_dir, bundle_manifest_path)
        elif protocol_id.startswith("optimus"):
            cell_lines, expression_lines = self._parse_optimus_bundle(bundle_dir, bundle_manifest_path)
        else:
            raise MatrixException(400, f"Failed to parse cell and expression metadata. "
                                       f"Unsupported analysis protocol {protocol_id}.")

        return (TableName.CELL, cell_lines, bundle_dir), (TableName.EXPRESSION, expression_lines, bundle_dir)

    def _parse_ss2_bundle(self, bundle_dir, bundle_manifest_path):
        """
        Parses SS2 analysis files into PSV rows for cell and expression Redshift tables.
        """
        # Get the keys associated with this cell, except for cellkey
        keys = self._parse_keys(bundle_dir)
        cell_key = keys["cell_suspension_key"]

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

        file_uuid = [f for f in json.load(open(bundle_manifest_path))["files"]
                     if f["name"].endswith(".genes.results")][0]["uuid"]
        file_version = [f for f in json.load(open(bundle_manifest_path))["files"]
                        if f["name"].endswith(".genes.results")][0]["version"]

        cell_lines = ['|'.join([
            cell_key,
            cell_key,
            keys["project_key"],
            keys["library_key"],
            keys["analysis_key"],
            file_uuid,
            file_version,
            "",
            str(genes_detected),
            "",
            ""]) + '\n']

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

    def _parse_optimus_bundle(self, bundle_dir, bundle_manifest_path):
        """
        Parses optimus analysis files into PSV rows for cell and expression Redshift tables.
        """

        keys = self._parse_keys(bundle_dir)

        file_uuid = [f for f in json.load(open(bundle_manifest_path))["files"]
                     if f["name"].endswith(".zattrs")][0]["uuid"]
        file_version = [f for f in json.load(open(bundle_manifest_path))["files"]
                        if f["name"].endswith(".zattrs")][0]["version"]

        emptydrops_result = {}
        with open(os.path.join(bundle_dir, "empty_drops_result.csv")) as emptydrops_file:
            reader = csv.DictReader(emptydrops_file)
            for row in reader:
                emptydrops_result[row["CellId"]] = {"total_umi_count": int(row["Total"]),
                                                    "is_cell": row["IsCell"] == "TRUE"}
        # read expression matrix from zarr
        store = DCPZarrStore(bundle_dir=bundle_dir)
        root = zarr.group(store=store)

        n_cells = root.cell_id.shape[0]
        chunk_size = root.cell_id.chunks[0]
        n_chunks = root.cell_id.nchunks
        cell_lines = set()
        expression_lines = []

        logger.info(f"Optimus bundle has {n_cells} cells and {n_chunks} chunks.")
        for i in range(n_chunks):
            self._parse_optimus_chunk(
                keys=keys,
                file_uuid=file_uuid,
                file_version=file_version,
                root=root,
                start_row=chunk_size * i,
                end_row=(i + 1) * chunk_size if (i + 1) * chunk_size < n_cells else n_cells,
                cell_lines=cell_lines,
                expression_lines=expression_lines,
                emptydrops_result=emptydrops_result
            )

        return cell_lines, expression_lines

    def _parse_optimus_chunk(self,
                             keys: dict,
                             file_uuid: str,
                             file_version: str,
                             root: zarr.Group,
                             start_row: int,
                             end_row: int,
                             cell_lines: set,
                             expression_lines: list,
                             emptydrops_result: dict):
        """
        Parses a chunk of a zarr group containing an expression matrix into cell and expression PSV lines.
        Modifies cell_lines and expression_lines.
        :param keys: Metadata keys generated by _parse_keys
        :param file_uuid: UUID of the file used for joining with rest of HCA metadata
        :param file_version: Version of the file used for joining with rest of HCA metadata
        :param root: Zarr group of the full expression matrix
        :param start_row: Start row of the chunk
        :param end_row: End row of the chunk
        :param cell_lines: Output cell PSV lines
        :param expression_lines: Output expression PSV lines
        :param emptydrops_result: Dict from cell barcode to UMI count and emptydrops call
        """
        logger.info(f"Parsing rows {start_row} to {end_row}.")
        chunk_size = end_row - start_row
        expr_values = root.expression[start_row:end_row]
        barcodes = root.cell_id[start_row:end_row]

        gene_ids = numpy.array([g.split(".")[0] for g in root.gene_id])
        for i in range(chunk_size):
            if emptydrops_result[barcodes[i]]["total_umi_count"] < self.emptydrops_min_count:
                continue
            cell_key = self._generate_10x_cell_key(keys["bundle_uuid"], barcodes[i])
            gene_count = numpy.count_nonzero(expr_values[i])
            cell_line = '|'.join(
                [cell_key,
                 keys["cell_suspension_key"],
                 keys["project_key"],
                 keys["library_key"],
                 keys["analysis_key"],
                 file_uuid,
                 file_version,
                 barcodes[i],
                 str(gene_count),
                 str(emptydrops_result[barcodes[i]]["total_umi_count"]),
                 't' if emptydrops_result[barcodes[i]]["is_cell"] else 'f']
            ) + '\n'
            cell_lines.add(cell_line)

            cell_expr_values = expr_values[i]
            nonzero_gene_ids = gene_ids[cell_expr_values != 0]
            nonzero_cevs = cell_expr_values[cell_expr_values != 0]

            for j in range(nonzero_gene_ids.shape[0]):
                expression_line = '|'.join(
                    [cell_key,
                     nonzero_gene_ids[j],
                     "Count",
                     str(nonzero_cevs[j])]
                ) + '\n'
                expression_lines.append(expression_line)

    def _generate_10x_cell_key(self, bundle_uuid, barcode):
        """
        Generate a unique hash for a cell.
        :param bundle_uuid: Bundle UUID the cell belongs to
        :param barcode: 10X cell barcode
        :return: MD5 hash
        """
        h = hashlib.md5()
        h.update(bundle_uuid.encode())
        h.update(barcode.encode())
        return h.hexdigest()

    def _parse_keys(self, bundle_dir):
        p = pathlib.Path(bundle_dir)

        bundle_uuid = pathlib.Path(bundle_dir).parts[-1]

        cs_path = p.joinpath("cell_suspension_0.json")
        cs_key = json.load(open(cs_path))['provenance']['document_id']

        project_path = p.joinpath("project_0.json")
        project_key = json.load(open(project_path))["provenance"]["document_id"]

        ap_path = p.joinpath("analysis_protocol_0.json")
        ap_key = json.load(open(ap_path))["provenance"]["document_id"]

        library_paths = list(p.glob("library_preparation_protocol_*.json"))
        library_keys = [json.load(open(p))['provenance']['document_id'] for p in library_paths]
        library_key = sorted(library_keys)[0]

        return {
            "bundle_uuid": bundle_uuid,
            "cell_suspension_key": cs_key,
            "project_key": project_key,
            "library_key": library_key,
            "analysis_key": ap_key
        }

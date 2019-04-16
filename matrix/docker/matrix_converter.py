"""Script to convert the outputs of Redshift queries into different formats."""

import argparse
import json
import os
import sys
import zipfile

import loompy
import pandas
import s3fs
import scipy.io
import scipy.sparse

from matrix.common import date
from matrix.common.logging import Logging
from matrix.common.constants import MatrixFormat
from matrix.common.request.request_tracker import RequestTracker, Subtask

LOGGER = Logging.get_logger(__file__)
SUPPORTED_FORMATS = [item.value for item in MatrixFormat]
TABLE_COLUMN_TO_METADATA_FIELD = {
    'cell_suspension_id': 'cell_suspension.provenance.document_id',
    'specimenkey': 'specimen_from_organism.provenance.document_id',
    'genus_species_ontology': 'specimen_from_organism.genus_species.ontology',
    'genus_species_label': 'specimen_from_organism.genus_species.ontology_label',
    'ethnicity_ontology': 'donor_organism.human_specific.ethnicity.ontology',
    'ethnicity_label': 'donor_organism.human_specific.ethnicity.ontology_label',
    'disease_ontology': 'donor_organism.diseases.ontology',
    'disease_label': 'donor_organism.diseases.ontology_label',
    'development_stage_ontology': 'donor_organism.development_stage.ontology',
    'development_stage_label': 'donor_organism.development_stage.ontology_label',
    'organ_ontology': 'derived_organ_ontology',
    'organ_label': 'derived_organ_label',
    'organ_part_ontology': 'derived_organ_part_ontology',
    'organ_part_label': 'derived_organ_part_label',
    'librarykey': 'library_preparation_protocol.provenance.document_id',
    'input_nucleic_acid_ontology': 'library_preparation_protocol.input_nucleic_acid_molecule.ontology',
    'input_nucleic_acid_label': 'library_preparation_protocol.input_nucleic_acid_molecule.ontology_label',
    'construction_approach_ontology': 'library_preparation_protocol.library_construction_method.ontology',
    'construction_approach_label': 'library_preparation_protocol.library_construction_method.ontology_label',
    'end_bias': 'library_preparation_protocol.end_bias',
    'strand': 'library_preparation_protocol.strand',
    'short_name': 'project.project_core.project_short_name'
}


class MatrixConverter:

    def __init__(self, args):
        self.args = args
        self.format = args.format
        self.request_tracker = RequestTracker(args.request_id)
        self.expression_manifest = None
        self.cell_manifest = None
        self.gene_manifest = None

        self.local_output_filename = os.path.basename(os.path.normpath(args.target_path))
        self.target_path = args.target_path
        self.FS = s3fs.S3FileSystem()

        Logging.set_correlation_id(LOGGER, value=args.request_id)

    def run(self):
        try:
            LOGGER.debug(f"Beginning matrix conversion run for {self.args.request_id}")
            self.expression_manifest = self._parse_manifest(self.args.expression_manifest_key)
            self.cell_manifest = self._parse_manifest(self.args.cell_metadata_manifest_key)
            self.gene_manifest = self._parse_manifest(self.args.gene_metadata_manifest_key)

            LOGGER.debug(f"Beginning conversion to {self.format}")
            local_converted_path = getattr(self, f"_to_{self.format}")()
            LOGGER.debug(f"Conversion to {self.format} completed")

            LOGGER.debug(f"Beginning upload to S3")
            self._upload_converted_matrix(local_converted_path, self.target_path)
            LOGGER.debug("Upload to S3 complete, job finished")

            self.request_tracker.complete_subtask_execution(Subtask.CONVERTER)
            self.request_tracker.complete_request(duration=(date.get_datetime_now() -
                                                  date.to_datetime(self.request_tracker.creation_date))
                                                  .total_seconds())
        except Exception as e:
            LOGGER.info(f"Matrix Conversion failed on {self.args.request_id} with error {str(e)}")
            self.request_tracker.log_error(str(e))
            raise e

    def _parse_manifest(self, manifest_key):
        """Parse a manifest file produced by a Redshift UNLOAD query.

        Args:
            manifest_key: S3 location of the manifest file.

        Returns:
            dict with three keys:
                "columns": the column headers for the tables
                "part_urls": full S3 urls for the files containing results from each
                    Redshift slice
                "record_count": total number of records returned by the query
        """
        manifest = json.load(self.FS.open(manifest_key))

        return {
            "columns": [e["name"] for e in manifest["schema"]["elements"]],
            "part_urls": [e["url"] for e in manifest["entries"] if e["meta"]["record_count"]],
            "record_count": manifest["meta"]["record_count"]
        }

    def _load_table(self, manifest, index_col=None):
        """Function to read all the manifest parts and return
        the concatenated dataframe

        Args:
            manifest: parsed manifest from _parse_manifest
            index_col (optional): column to set as the dataframe index

        Returns:
            concatenated DataFrame
        """

        dfs = self._load_table_by_part(manifest)
        return pandas.concat(dfs, copy=False)

    def _load_table_by_part(self, manifest, index_col=None):
        """Generator to read each table part file specified in a manifest and yield
        dataframes for each part.

        Args:
            manifest: parsed manifest from _parse_manifest
            index_col (optional): column to set as the dataframe index

        Yields:
            Dataframe read from one slice's Redshift output.
        """

        columns = self._map_columns(manifest['columns'])
        for part_url in manifest["part_urls"]:
            df = pandas.read_csv(part_url, sep='|', header=None, names=columns,
                                 true_values=["t"], false_values=["f"],
                                 index_col=index_col)
            yield df

    def _map_columns(self, cols):
        return [TABLE_COLUMN_TO_METADATA_FIELD[col]
                if col in TABLE_COLUMN_TO_METADATA_FIELD else col
                for col in cols]

    def _to_mtx(self):
        """Write a zip file with an mtx and two metadata tsvs from Redshift query
        manifests.

        Returns:
           output_path: Path to the zip file.
        """

        # Add zip to the output filename and create the directory where we will
        # write output files.
        if not self.local_output_filename.endswith(".zip"):
            self.local_output_filename += ".zip"
        results_dir = os.path.splitext(self.local_output_filename)[0]
        os.mkdir(results_dir)

        # Load the gene metadata and write it out to a tsv
        gene_df = self._load_table(self.gene_manifest, index_col="featurekey")
        gene_df.to_csv(os.path.join(results_dir, "genes.tsv"), sep='\t')

        # Read the row (gene) attributes and then set some conventional names
        row_attrs = self._load_table(self.gene_manifest).to_dict("series")
        # Not expected to be unique
        row_attrs["Gene"] = row_attrs.pop("featurename")
        row_attrs["Accession"] = row_attrs.pop("featurekey")

        cellkeys = pandas.Index([])
        sparse_expression_cscs = []

        for expression_part in self._load_table_by_part(self.expression_manifest,
                                                        index_col=["featurekey", "cellkey"]):
            # Pivot the cells to columns and fill in the missing gene values with
            # zeros
            unstacked = expression_part.unstack("cellkey")
            unstacked.columns = unstacked.columns.get_level_values(1)
            sparse_filled = scipy.sparse.csc_matrix(unstacked.reindex(index=row_attrs["Accession"]).fillna(0))
            sparse_expression_cscs.append(sparse_filled)
            cellkeys = cellkeys.union(unstacked.columns)

        # Write out concatenated expression matrix
        big_sparse_matrix = scipy.sparse.hstack(sparse_expression_cscs)
        scipy.io.mmwrite(os.path.join(results_dir, "matrix.mtx"),
                         big_sparse_matrix.astype('f'))

        # Read the cell metadata, reindex by the cellkeys in the expression matrix,
        # and write to another tsv
        cell_df = self._load_table(self.cell_manifest, index_col="cellkey")
        cell_df.index = cellkeys
        cell_df.to_csv(os.path.join(results_dir, "cells.tsv"), sep='\t')

        # Create a zip file out of the three written files.
        zipf = zipfile.ZipFile(self.local_output_filename, 'w', zipfile.ZIP_DEFLATED)
        zipf.write(os.path.join(results_dir, "genes.tsv"))
        zipf.write(os.path.join(results_dir, "matrix.mtx"))
        zipf.write(os.path.join(results_dir, "cells.tsv"))
        zipf.write("mtx_readme.txt")

        return self.local_output_filename

    def _to_loom(self):
        """Write a loom file from Redshift query manifests.

        Returns:
           output_path: Path to the new loom file.
        """

        # Put loom on the output filename if it's not already there.
        if not self.local_output_filename.endswith(".loom"):
            self.local_output_filename += ".loom"

        # Read the row (gene) attributes and then set some conventional names
        row_attrs = self._load_table(self.gene_manifest).to_dict("series")
        # Not expected to be unique
        row_attrs["Gene"] = row_attrs.pop("featurename")
        row_attrs["Accession"] = row_attrs.pop("featurekey")
        cellkeys = pandas.Index([])
        sparse_expression_cscs = []

        for expression_part in self._load_table_by_part(self.expression_manifest,
                                                        index_col=["featurekey", "cellkey"]):
            # Pivot the cellkey index to columns and tidy up the resulting
            # multi-level columns
            unstacked = expression_part.unstack("cellkey")
            unstacked.columns = unstacked.columns.get_level_values(1)

            # Reindex with the list of gene ids, filling in zeros for unobserved
            # genes. The makes the martrix dense but assigns correct indices when
            # we sparsify it.
            sparse_filled = scipy.sparse.csc_matrix(unstacked.reindex(index=row_attrs["Accession"])
                                                    .fillna(0))
            sparse_expression_cscs.append(sparse_filled)

            # Keep track of the cellkeys as we observe them so we can later
            # correctly order the column attributes.
            cellkeys = cellkeys.union(unstacked.columns)

        big_sparse_matrix = scipy.sparse.hstack(sparse_expression_cscs)

        # Read in the cell metadata and reindex by the cellkeys from the expression
        # matrix. Set the "CellID" label convention from the loom docs.
        cell_df = self._load_table(self.cell_manifest, index_col="cellkey")
        cell_df.index = cellkeys
        cell_df["cellkey"] = cell_df.index
        col_attrs = cell_df.to_dict("series")
        col_attrs["CellID"] = col_attrs.pop("cellkey")

        # I don't know, you have to do this or it doesn't work.
        for key, val in col_attrs.items():
            col_attrs[key] = val.values
        for key, val in row_attrs.items():
            row_attrs[key] = val.values
        loompy.create(self.local_output_filename, big_sparse_matrix, row_attrs, col_attrs)

        return self.local_output_filename

    def _to_csv(self):
        """Write a zip file with csvs from Redshift query manifests and readme.

        Returns:
           output_path: Path to the new zip file.
        """

        if not self.local_output_filename.endswith(".zip"):
            self.local_output_filename += ".zip"

        results_dir = os.path.splitext(self.local_output_filename)[0]
        os.mkdir(results_dir)

        gene_df = self._load_table(self.gene_manifest, index_col="featurekey")
        gene_df.to_csv(os.path.join(results_dir, "genes.csv"))

        # Read the row (gene) attributes and then set some conventional names
        row_attrs = self._load_table(self.gene_manifest).to_dict("series")
        # Not expected to be unique
        row_attrs["Gene"] = row_attrs.pop("featurename")
        row_attrs["Accession"] = row_attrs.pop("featurekey")

        cellkeys = pandas.Index([])
        with open(os.path.join(results_dir, "expression.csv"), "w") as exp_f:
            gene_index_string_list = [str(x) for x in gene_df.index.tolist()]
            exp_f.write(','.join(["cellkey"] + gene_index_string_list))
            exp_f.write('\n')

            for expression_part in self._load_table_by_part(self.expression_manifest,
                                                            index_col=["cellkey", "featurekey"]):
                unstacked = expression_part.unstack()
                unstacked.columns = unstacked.columns.get_level_values(1)
                filled = unstacked.reindex(columns=row_attrs["Accession"]).fillna(0)
                filled.to_csv(exp_f, header=False, float_format='%g')
                cellkeys = cellkeys.union(filled.index)
        cell_df = self._load_table(self.cell_manifest, index_col="cellkey")
        cell_df.index = cellkeys
        cell_df.to_csv(os.path.join(results_dir, "cells.csv"))

        zipf = zipfile.ZipFile(self.local_output_filename, 'w', zipfile.ZIP_DEFLATED)
        zipf.write(os.path.join(results_dir, "genes.csv"))
        zipf.write(os.path.join(results_dir, "expression.csv"))
        zipf.write(os.path.join(results_dir, "cells.csv"))
        zipf.write("csv_readme.txt")

        return self.local_output_filename

    def _upload_converted_matrix(self, local_path, remote_path):
        """
        Upload the converted matrix to S3.
        Parameters
        ----------
        local_path : str
            Path to the new, converted matrix file
        remote_path : str
            S3 path where the converted matrix will be uploaded
        """
        self.FS.put(local_path, remote_path)


def main(args):
    """Entry point."""

    parser = argparse.ArgumentParser()
    parser.add_argument("request_id",
                        help="ID of the request associated with this conversion.")
    parser.add_argument("expression_manifest_key",
                        help="S3 url to Redshift manifest for the expression table.")
    parser.add_argument("cell_metadata_manifest_key",
                        help="S3 url to Redshift manifest for the cell table.")
    parser.add_argument("gene_metadata_manifest_key",
                        help="S3 url to Redshift manifest for the gene table.")
    parser.add_argument("target_path",
                        help="S3 prefix where the file should be written.")
    parser.add_argument("format",
                        help="Target format for conversion",
                        choices=SUPPORTED_FORMATS)
    args = parser.parse_args(args)
    LOGGER.debug(
        f"Starting matrix conversion job with parameters: "
        f"{args.expression_manifest_key}, {args.cell_metadata_manifest_key}, "
        f"{args.gene_metadata_manifest_key}, {args.target_path}, {args.format}")

    matrix_converter = MatrixConverter(args)
    matrix_converter.run()

if __name__ == "__main__":
    print(f"STARTED with argv: {sys.argv}")
    main(sys.argv[1:])

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

from matrix.common.logging import Logging

LOGGER = Logging.get_logger(__file__)
FS = s3fs.S3FileSystem()


# These are the formats that users can request.
SUPPORTED_FORMATS = [
    "mtx",
    "csv",
    "loom"
]

def parse_manifest(prefix):
    """Parse a manifest file produced by a Redshift UNLOAD query.

    Args:
        prefix: S3 prefix of the manifest file. This is what appears in the TO clause
        of the UNLOAD query.

    Returns:
        dict with three keys:
            "columns": the column headers for the tables
            "part_urls": full S3 urls for the files containing results from each
                Redshift slice
            "record_count": total number of records returned by the query
    """

    manifest_key = prefix + "manifest"
    manifest = json.load(FS.open(manifest_key))

    return {
        "columns": [e["name"] for e in manifest["schema"]["elements"]],
        "part_urls": [e["url"] for e in manifest["entries"] if e["meta"]["record_count"]],
        "record_count": manifest["meta"]["record_count"]
    }

def load_table_by_part(manifest, index_col=None):
    """Generator to read each table part file specified in a manifest and yield
    dataframes for each part.

    Args:
        manifest: parsed manifest from parse_manifest
        index_col (optional): column to set as the dataframe index

    Yields:
        Dataframe read from one slice's Redshift output.
    """

    columns = manifest["columns"]
    for part_url in manifest["part_urls"]:
        df = pandas.read_csv(part_url, sep='|', header=None, names=columns,
                             true_values=["t"], false_values=["f"],
                             index_col=index_col)
        yield df

def load_table(manifest, index_col=None):
    """Same as load_table_by_part, but read all the parts at once and return
    the concatenated dataframe.
    """

    dfs = []
    columns = manifest["columns"]
    for part_url in manifest["part_urls"]:
        df = pandas.read_csv(part_url, sep='|', header=None, names=columns,
                             true_values=["t"], false_values=["f"],
                             index_col=index_col)
        dfs.append(df)

    return pandas.concat(dfs, copy=False)

def to_mtx(expression_manifest, cell_manifest, gene_manifest, output_filename):
    """Write a zip file with an mtx and two metadata tsvs from Redshift query
    manifests.

    Args:
        expression_manifest, cell_manifest, gene_manifest: S3 URLs to the
            manifest files created by the Redshift UNLOAD query.
        output_filename: Name of the loom file to create.

    Returns:
       output_path: Path to the zip file.
    """

    # Add zip to the output filename and create the directory where we will
    # write output files.
    if not output_filename.endswith(".zip"):
        output_filename += ".zip"
    results_dir = os.path.splitext(output_filename)[0]
    os.mkdir(results_dir)

    # Load the gene metadata and write it out to a tsv
    gene_df = load_table(gene_manifest, index_col="featurekey")
    gene_df.to_csv(os.path.join(results_dir, "genes.tsv"), sep='\t')

    cellkeys = pandas.Index([])
    sparse_expression_cscs = []

    for expression_part in load_table_by_part(expression_manifest,
                                              index_col=["featurekey", "cellkey"]):
        # Pivot the cells to columns and fill in the missing gene values with
        # zeros
        unstacked = expression_part.unstack("cellkey")
        unstacked.columns = unstacked.columns.get_level_values(1)
        sparse_filled = scipy.sparse.csc_matrix(unstacked.reindex(index=gene_df.index)
                                                .fillna(0).round(2))
        sparse_expression_cscs.append(sparse_filled)
        cellkeys = cellkeys.union(unstacked.columns)

    # Write out concatenated expression matrix
    big_sparse_matrix = scipy.sparse.hstack(sparse_expression_cscs)
    scipy.io.mmwrite(os.path.join(results_dir, "matrix.mtx"),
                     big_sparse_matrix.astype('f'), precision=2)

    # Read the cell metadata, reindex by the cellkeys in the expression matrix,
    # and write to another tsv
    cell_df = load_table(cell_manifest, index_col="cellkey")
    cell_df.reindex(index=cellkeys)
    cell_df.to_csv(os.path.join(results_dir, "cells.tsv"), sep='\t')

    # Create a zip file out of the three written files.
    zipf = zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED)
    zipf.write(os.path.join(results_dir, "genes.tsv"))
    zipf.write(os.path.join(results_dir, "matrix.mtx"))
    zipf.write(os.path.join(results_dir, "cells.tsv"))

    return output_filename

def to_loom(expression_manifest, cell_manifest, gene_manifest, output_filename):
    """Write a loom file from Redshift query manifests.

    Args:
        expression_manifest, cell_manifest, gene_manifest: S3 URLs to the
            manifest files created by the Redshift UNLOAD query.
        output_filename: Name of the loom file to create.

    Returns:
       output_path: Path to the new loom file.
    """

    # Put loom on the output filename if it's not already there.
    if not output_filename.endswith(".loom"):
        output_filename += ".loom"

    # Read the row (gene) attributes and then set some conventional names
    row_attrs = load_table(gene_manifest).to_dict("series")
    row_attrs["Gene"] = row_attrs.pop("featurename") # Not expected to be unique
    row_attrs["Accession"] = row_attrs.pop("featurekey")

    cellkeys = pandas.Index([])
    sparse_expression_cscs = []

    for expression_part in load_table_by_part(expression_manifest,
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
    cell_df = load_table(cell_manifest, index_col="cellkey")
    cell_df.reindex(index=cellkeys)
    cell_df["cellkey"] = cell_df.index
    col_attrs = cell_df.to_dict("series")
    col_attrs["CellID"] = col_attrs.pop("cellkey")

    # I don't know, you have to do this or it doesn't work.
    for key, val in col_attrs.items():
        col_attrs[key] = val.values
    for key, val in row_attrs.items():
        row_attrs[key] = val.values
    loompy.create(output_filename, big_sparse_matrix, row_attrs, col_attrs)

    return output_filename

def to_csv(expression_manifest, cell_manifest, gene_manifest, output_filename):
    """Write a zip file with three csvs from Redshift query manifests.

    Args:
        expression_manifest, cell_manifest, gene_manifest: S3 URLs to the
            manifest files created by the Redshift UNLOAD query.
        output_filename: Name of the loom file to create.

    Returns:
       output_path: Path to the new zip file.
    """

    if not output_filename.endswith(".zip"):
        output_filename += ".zip"

    results_dir = os.path.splitext(output_filename)[0]
    os.mkdir(results_dir)

    gene_df = load_table(gene_manifest, index_col="featurekey")
    gene_df.to_csv(os.path.join(results_dir, "genes.csv"))

    cellkeys = pandas.Index([])
    with open(os.path.join(results_dir, "expression.csv"), "w") as exp_f:
        exp_f.write(','.join(["cellkey"] + gene_df.index.tolist()))
        exp_f.write('\n')

        for expression_part in load_table_by_part(expression_manifest,
                                                  index_col=["cellkey", "featurekey"]):
            unstacked = expression_part.unstack()
            unstacked.columns = unstacked.columns.get_level_values(1)
            filled = unstacked.reindex(columns=gene_df.index).fillna(0)
            filled.to_csv(exp_f, header=False, float_format='%g')
            cellkeys = cellkeys.union(filled.index)
    cell_df = load_table(cell_manifest, index_col="cellkey")
    cell_df.reindex(index=cellkeys)
    cell_df.to_csv(os.path.join(results_dir, "cells.csv"))

    zipf = zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED)
    zipf.write(os.path.join(results_dir, "genes.csv"))
    zipf.write(os.path.join(results_dir, "expression.csv"))
    zipf.write(os.path.join(results_dir, "cells.csv"))

    return output_filename


def upload_converted_matrix(local_path, remote_path):
    """
    Upload the converted matrix to S3.
    Parameters
    ----------
    local_path : str
        Path to the new, converted matrix file
    remote_path : str
        S3 path where the converted matrix will be uploaded
    """

    FS.put(local_path, remote_path)


def main(args):
    """Entry point."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--expression-manifest",
                        help="S3 url to Redshift manifest for the expression table.",
                        required=True)
    parser.add_argument("--cell-metadata-manifest",
                        help="S3 url to Redshift manifest for the cell table.",
                        required=True)
    parser.add_argument("--gene-metadata-manifest",
                        help="S3 url to Redshift manifest for the gene table.",
                        required=True)
    parser.add_argument("--output-filename",
                        help="Name of the output file to produce.",
                        required=True)
    parser.add_argument("--target-path",
                        help="S3 prefix where the file should be written.",
                        required=True)
    parser.add_argument("--format",
                        help="Target format for conversion",
                        choices=SUPPORTED_FORMATS)
    args = parser.parse_args()

    LOGGER.debug(
        f"Starting matrix conversion job with parameters: "
        f"{args.expression_manifest}, {args.cell_metadata_manifest}, "
        f"{args.gene_metadata_manifest}, {args.output_filename}, "
        f"{args.target_path}, {args.format}")

    expression_manifest = parse_manifest(args.expression_manifest)
    cell_manifest = parse_manifest(args.cell_metadata_manifest)
    gene_manifest = parse_manifest(args.gene_metadata_manifest)

    local_converted_path = globals()["to_" + args.format](
        expression_manifest, cell_manifest, gene_manifest, args.output_filename)
    LOGGER.debug(f"Conversion to {args.format} complete, beginning upload to S3")

    upload_converted_matrix(
        local_converted_path,
        os.path.join(args.target_path, local_converted_path))
    LOGGER.debug("Upload to S3 complete, job finished")

    # Logic for updating status tables, cloudwatch, etc should go here.

if __name__ == "__main__":
    print(f"STARTED with argv: {sys.argv}")
    main(sys.argv[1:])

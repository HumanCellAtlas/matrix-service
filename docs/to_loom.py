"""Example of possible parallelization of the creation of a loom."""
import argparse
import concurrent.futures
import json
import os

import loompy
import pandas as pd
import s3fs


EXPRESSION = "expression"
CELL_METADATA = "cell_metadata"
GENE_METADATA = "gene_metadata"
GENE_PREFIX = "hca-matrix-redshift-results/conversion_test"

FS = s3fs.S3FileSystem(default_block_size=100 * 2**20)

def parse_manifest(prefix):
    manifest_key = prefix + "manifest"
    manifest = json.load(FS.open(manifest_key))

    return {
        "columns": [e["name"] for e in manifest["schema"]["elements"]],
        "part_urls": [e["url"] for e in manifest["entries"] if e["meta"]["record_count"]],
        "part_record_counts": [e["meta"]["record_count"] for e in manifest["entries"] if e["meta"]["record_count"]],
        "record_count": manifest["meta"]["record_count"]
    }

def load_table_by_part(manifest, index_col=None):
    """In this implementation, don't actually load the table here. This is going
    to get passed to parse_expression_part.
    """
    columns = manifest["columns"]
    for part_url, part_record_count in zip(manifest["part_urls"], manifest["part_record_counts"]):
        print(columns, flush=True)
        yield part_url, columns

def load_table(manifest, index_col=None):
    dfs = []
    columns = manifest["columns"]
    for part_url in manifest["part_urls"]:
        df = pd.read_csv(part_url, sep='|', header=None, names=columns,
                         true_values=["t"], false_values=["f"],
                         index_col=index_col)
        dfs.append(df)

    return pd.concat(dfs, copy=False)

def parse_expression_part(genes, part_url, columns):

    # Read in the PSV. This is kind of big when looking at the whole current
    # HCA but like ~6GB.
    df = pd.read_csv(part_url, sep='|', header=None, names=columns,
                     true_values=["t"], false_values=["f"])

    # Groupby cell, so we can iterate over each cell and reshape the PSV into
    # what we are expecting.
    grouped = df.groupby("cellkey")
    sparse_cell_dfs = []

    for cell_group in grouped:
        # Iterating through theses gives you (cell_id, cell_dataframe) tuples.
        cell_df = cell_group[1]

        # The pivot method reshapes the dataframe so the cellkey moves to a
        # column label.
        # The reindex method reorders the features so they're identical across
        # all dataframes
        # to_sparse drops all the zero entries, which can be ~90% of the
        # matrix.
        # It seems like you would want to do this with more than one cell at a
        # time, but the various things I've tried lead to big memory blowups
        # from pandas.
        sparse_cell_dfs.append(cell_df.pivot(index="featurekey",
                                             columns="cellkey",
                                             values="exrpvalue").reindex(index=genes).to_sparse())

    # Concatenate all the sparse matrices together. This should still preserve
    # sparsity.
    concatenated_df = pd.concat(sparse_cell_dfs, axis=1)

    return concatenated_df

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-prefix", required=True)
    parser.add_argument("--output-filename", required=True)
    args = parser.parse_args()

    # Load the results manifests. These contain the file schemas and the paths
    # to the parts.
    cell_metadata_manifest = parse_manifest(os.path.join(args.s3_prefix, CELL_METADATA))
    gene_metadata_manifest = parse_manifest(os.path.join(GENE_PREFIX, GENE_METADATA))
    expression_manifest = parse_manifest(os.path.join(args.s3_prefix, EXPRESSION))


    row_attrs = load_table(gene_metadata_manifest).to_dict("series")

    # Set the conventional names for the gene attributes
    row_attrs["Gene"] = row_attrs.pop("featurename") # Not expected to be unique
    row_attrs["Accession"] = row_attrs.pop("featurekey")

    sparse_expression_dfs = []

    table_parts = list(load_table_by_part(expression_manifest))

    # Each output file from the query can be processed in parallel, as long as
    # cells are fully contained within one part.
    with concurrent.futures.ProcessPoolExecutor(4) as exe:
        futures = [exe.submit(parse_expression_part, row_attrs["Accession"], *expression_part) for expression_part in
                   table_parts]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            sparse_expression_dfs.append(result)

    big_sparse_matrix = pd.concat(sparse_expression_dfs, axis=1)
    del sparse_expression_dfs
    cell_df = load_table(cell_metadata_manifest, index_col="cellkey")

    # Make sure the cell metadata dataframe lines up with the expression
    # dataframe.
    cell_df = cell_df.reindex(index=big_sparse_matrix.columns)

    # Set the loom CellID convention.
    cell_df["cellkey"] = cell_df.index
    col_attrs = cell_df.to_dict("series")
    col_attrs["CellID"] = col_attrs.pop("cellkey")

    # I don't know...
    for k, v in col_attrs.items():
        col_attrs[k] = v.values
    for k, v in row_attrs.items():
        row_attrs[k] = v.values
    coo_matrix = big_sparse_matrix.to_coo()
    loompy.create(args.output_filename, coo_matrix, row_attrs, col_attrs)

if __name__ == '__main__':
    main()

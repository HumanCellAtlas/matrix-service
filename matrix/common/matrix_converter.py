"""Script to convert the zarr-formatted expression matrices into other formats."""

import sys
import argparse
import os
import tarfile
import tempfile

import loompy
import numpy
import pandas
import s3fs
import scipy.io
import zarr

from matrix.common.request_tracker import RequestTracker, Subtask

# These are the formats that users can request.
SUPPORTED_FORMATS = [
    "mtx",
    "csv",
    "loom"
]


def zarr_to_loom(zarr_root):
    """
    Convert a zarr group to a loom file.
    Writes the loom file to a local temp directory. Puts the metadata into
    loom structures.
    Parameters
    ----------
    zarr_root : str
        S3 path to the root of the zarr store.
    Returns
    -------
    str
        Local path to the converted loom file.
    """

    temp_dir = tempfile.mkdtemp()
    loom_path = os.path.join(temp_dir, "matrix.loom")

    # Set the cell metadata as loom row attrs
    row_attrs = {}
    row_attrs["CellID"] = zarr_root.cell_id[:]
    for cell_metadata_idx in range(zarr_root.cell_metadata_numeric_name.shape[0]):
        metadata_name = zarr_root.cell_metadata_numeric_name[cell_metadata_idx]
        row_attrs[metadata_name] = \
            zarr_root.cell_metadata_numeric[:, cell_metadata_idx]
    for cell_metadata_idx in range(zarr_root.cell_metadata_string_name.shape[0]):
        metadata_name = zarr_root.cell_metadata_string_name[cell_metadata_idx]
        row_attrs[metadata_name] = \
            zarr_root.cell_metadata_string[:, cell_metadata_idx]

    # Set the gene metadata as loom col attrs
    col_attrs = {}
    col_attrs["Gene"] = zarr_root.gene_id[:]
    try:
        for gene_metadata_idx in range(zarr_root.gene_metadata_name.shape[0]):
            metadata_name = zarr_root.gene_metadata_name[gene_metadata_idx]
            col_attrs[metadata_name] = \
                zarr_root.gene_metadata[:, gene_metadata_idx]
    except AttributeError:
        pass

    loompy.create(loom_path, zarr_root.expression[:].T, col_attrs, row_attrs)

    return loom_path


def zarr_to_csv(zarr_root):
    """
    Convert a zarr group to a gzipped csv file.
    This discards all of the metadata except gene_id and cell_id.
    Parameters
    ----------
    zarr_root : str
        S3 path to the root of the zarr store.
    Returns
    -------
    str
        Local path to the converted csv.gz file.
    """

    temp_dir = tempfile.mkdtemp()
    csv_path = os.path.join(temp_dir, "matrix.csv.gz")

    dataframe = pandas.DataFrame(zarr_root.expression[:], index=zarr_root.cell_id[:],
                                 columns=zarr_root.gene_id[:])
    dataframe.to_csv(csv_path, compression="gzip")
    return csv_path


def zarr_to_mtx(zarr_root):
    """
    Convert a zarr group to an mtx file.
    The expression values are placed into a matrix market mtx file. The
    gene_ids and cell_ids are put in separate csv files. This mimics how files
    are delivered to 10x.
    Parameters
    ----------
    zarr_root : str
        S3 path to the root of the zarr store.
    Returns
    -------
    str
        Local path to a tarbsall with the two csv files and the mtx file.
    """

    temp_dir = tempfile.mkdtemp()
    mtx_path = os.path.join(temp_dir, "matrix.mtx")
    dataframe = pandas.DataFrame(zarr_root.expression[:])

    sparse_mat = scipy.sparse.coo_matrix(dataframe.values)
    scipy.io.mmwrite(mtx_path, sparse_mat)

    cell_id_path = os.path.join(temp_dir, "cell_id.csv")
    gene_id_path = os.path.join(temp_dir, "gene_id.csv")
    numpy.savetxt(cell_id_path, zarr_root.cell_id[:], fmt="%s")
    numpy.savetxt(gene_id_path, zarr_root.gene_id[:], fmt="%s")

    tar_path = os.path.join(tempfile.mkdtemp(), "matrix.tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(temp_dir, arcname="matrix")

    return tar_path


def open_zarr(zarr_s3_path, anon=False):
    """
    Open a zarr store on S3.
    This should open an output of the matrix service that's been placed in S3.
    Parameters
    ----------
    zarr_s3_path : str
        S3 path to the root of the zarr store.
    anon : bool, optional
        Access the bucket anonymously. If False, use credentials determined by
        the environment. (default is False)
    Returns
    -------
    zarr.Group
        A zarr group object that refers to the root of the expression matrix
    """

    s3 = s3fs.S3FileSystem(anon=anon)
    store = s3fs.S3Map(zarr_s3_path, s3=s3, check=False, create=False)
    matrix_root = zarr.group(store=store)
    return matrix_root


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

    s3 = s3fs.S3FileSystem(anon=False)
    s3.put(local_path, remote_path)


def main(args):
    """Entry point."""

    parser = argparse.ArgumentParser()
    parser.add_argument("request_hash",
                        help=("Request hash of the filter merge job"))
    parser.add_argument("source_zarr_path",
                        help=("Path to the root of the zarr store containing "
                              "the matrix to be converted."))
    parser.add_argument("target_path",
                        help="Path where converted matrix should be written.")
    parser.add_argument("format",
                        help="Target format for conversion.",
                        choices=SUPPORTED_FORMATS)
    args = parser.parse_args(args)
    print(f"request hash of job is {args.request_hash}")
    print(f"source path of job is {args.source_zarr_path}")
    print(f"target path of job is {args.target_path}")
    print(f"expected format of job is {args.format}")

    zarr_root = open_zarr(args.source_zarr_path)
    print(f"Beginning conversion of job {args.request_hash}")
    local_converted_path = globals()["zarr_to_" + args.format](zarr_root)
    print(f"Completed conversion of job {args.request_hash}")

    print(f"Uploading converted matrix for job {args.request_hash}")
    upload_converted_matrix(local_converted_path, args.target_path)
    print(f"Uploaded converted matrix for job {args.request_hash}")
    RequestTracker(args.request_hash).complete_subtask_execution(Subtask.CONVERTER)


if __name__ == "__main__":
    print(f"STARTED with argv: {sys.argv}")
    main(sys.argv[1:])

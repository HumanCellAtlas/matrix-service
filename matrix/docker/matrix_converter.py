"""Script to convert the zarr-formatted expression matrices into other formats."""

import sys
import argparse
import os
import tempfile
import zipfile

import loompy
import numpy
import pandas
import s3fs
import scipy.io
import zarr

from matrix.common import date
from matrix.common.logging import Logging
from matrix.common.request.request_cache import RequestCache
from matrix.common.request.request_tracker import RequestTracker, Subtask
from matrix.common.aws.cloudwatch_handler import CloudwatchHandler, MetricName

logger = Logging.get_logger(__file__)

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
    csv_path = os.path.join(temp_dir, "matrix.csv")
    csv_zip_path = os.path.join(temp_dir, "matrix.csv.zip")

    dataframe = pandas.DataFrame(zarr_root.expression[:], index=zarr_root.cell_id[:],
                                 columns=zarr_root.gene_id[:])
    dataframe.to_csv(csv_path)
    with zipfile.ZipFile(csv_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        z.write(csv_path, arcname=os.path.basename(csv_path))

    return csv_zip_path


def zarr_to_mtx(zarr_root):
    """
    Convert a zarr group to an mtx zip file.
    The expression values are placed into a matrix market mtx file. The
    gene_ids and cell_ids are put in separate csv files. This (sort of) mimics
    how files are delivered from 10x.

    Parameters
    ----------
    zarr_root : str
        S3 path to the root of the zarr store.

    Returns
    -------
    str
        Local path to a zip archive with the two csv files and the mtx file.
    """
    temp_dir = tempfile.mkdtemp()
    mtx_path = os.path.join(temp_dir, "matrix.mtx")
    dataframe = pandas.DataFrame(zarr_root.expression[:])

    sparse_mat = scipy.sparse.coo_matrix(dataframe.values).transpose()
    scipy.io.mmwrite(mtx_path, sparse_mat)

    cell_id_path = os.path.join(temp_dir, "cell_id.csv")
    gene_id_path = os.path.join(temp_dir, "gene_id.csv")
    numpy.savetxt(cell_id_path, zarr_root.cell_id[:], fmt="%s")
    numpy.savetxt(gene_id_path, zarr_root.gene_id[:], fmt="%s")

    zip_path = os.path.join(tempfile.mkdtemp(), "matrix.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for output_path in os.listdir(temp_dir):
            zip_file.write(os.path.join(temp_dir, output_path),
                           arcname=os.path.join("matrix", output_path))
    return zip_path


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
    parser.add_argument("request_id",
                        help="Request id of the filter merge job")
    parser.add_argument("request_hash",
                        help="Request hash of the filter merge job")
    parser.add_argument("source_zarr_path",
                        help=("Path to the root of the zarr store containing "
                              "the matrix to be converted."))
    parser.add_argument("target_path",
                        help="Path where converted matrix should be written.")
    parser.add_argument("format",
                        help="Target format for conversion.",
                        choices=SUPPORTED_FORMATS)
    args = parser.parse_args(args)

    Logging.set_correlation_id(logger, value=args.request_hash)

    logger.debug(f"Starting matrix conversion job with parameters:"
                 f"{args.request_id}, {args.request_hash}, {args.source_zarr_path}, {args.target_path}, {args.format}")

    zarr_root = open_zarr(args.source_zarr_path)
    local_converted_path = globals()["zarr_to_" + args.format](zarr_root)
    logger.debug(f"Conversion to {args.format} complete, beginning upload to S3")

    upload_converted_matrix(local_converted_path, args.target_path)
    logger.debug("Upload to S3 complete, job finished")

    request_cache = RequestCache(args.request_id)
    request_tracker = RequestTracker(args.request_hash)
    request_tracker.complete_subtask_execution(Subtask.CONVERTER)

    cloudwatch_handler = CloudwatchHandler()
    cloudwatch_handler.put_metric_data(
        metric_name=MetricName.CONVERSION_COMPLETION,
        metric_value=1
    )
    request_tracker.complete_request(duration=(date.get_datetime_now() -
                                               date.to_datetime(request_cache.creation_date))
                                     .total_seconds())


if __name__ == "__main__":
    print(f"STARTED with argv: {sys.argv}")
    main(sys.argv[1:])

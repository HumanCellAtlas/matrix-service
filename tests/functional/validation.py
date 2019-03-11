import concurrent.futures
import csv
import io
import os
import shutil
import tempfile
import zipfile

import loompy
import numpy
import pandas
import requests
import s3fs
import scipy
import zarr

S3 = s3fs.S3FileSystem(anon=True)
DSS_CLIENT = s3fs.S3FileSystem(anon=True)


def calculate_ss2_metrics_direct(bundle_fqids):
    """Calculate expected SS2 matrix values.

    Don't use matrices or the matrix service, calculate in a completely orthogonal way
    using the RSEM outputs directly.
    """

    def read_bundle(fqid):
        bundle_uuid, bundle_version = fqid.split(".", 1)
        bundle = DSS_CLIENT.get_bundle(uuid=bundle_uuid, version=bundle_version, replica="aws")

        rsem_file = [f for f in bundle['bundle']['files']
                     if f["name"].endswith(".genes.results")][0]

        rsem_contents = DSS_CLIENT.get_file(uuid=rsem_file["uuid"],
                                            version=rsem_file["version"],
                                            replica="aws")

        rsem_reader = csv.DictReader(io.StringIO(rsem_contents.decode()), delimiter="\t")

        bundle_expression_sum = 0
        bundle_expression_nonzeros = set()
        for row in rsem_reader:
            bundle_expression_sum += float(row["expected_count"])
            if float(row['expected_count']) != 0.0:
                bundle_expression_nonzeros.add(row["gene_id"].split(".", 1)[0])
        return {"expression_sum": bundle_expression_sum,
                "expression_nonzero": len(bundle_expression_nonzeros),
                "cell_count": 1}

    expression_sum = 0
    expression_nonzero = 0
    cell_count = 0
    with concurrent.futures.ThreadPoolExecutor(16) as executor:
        futures = [executor.submit(read_bundle, fqid) for fqid in bundle_fqids]
        for future in concurrent.futures.as_completed(futures):
            data = future.result()
            expression_sum += data["expression_sum"]
            expression_nonzero += data["expression_nonzero"]
            cell_count += data["cell_count"]

    return {"expression_sum": expression_sum, "expression_nonzero": expression_nonzero,
            "cell_count": cell_count}


def calculate_ss2_metrics_zarr(s3_path):
    """Calculate metrics for a zarr store in s3."""

    store = s3fs.S3Map(s3_path, s3=S3, check=False, create=False)
    group = zarr.group(store=store)
    expression_sum = numpy.sum(group.expression[:])
    expression_nonzero = numpy.count_nonzero(group.expression[:])
    cell_count = group.cell_id.shape[0]

    return {"expression_sum": expression_sum, "expression_nonzero": expression_nonzero,
            "cell_count": cell_count}


def calculate_ss2_metrics_loom(loom_url):
    """Calculate metrics for a loom file."""

    temp_dir = tempfile.mkdtemp(suffix="loom_test")
    local_loom_path = os.path.join(temp_dir, os.path.basename(loom_url))
    response = requests.get(loom_url, stream=True)
    with open(local_loom_path, "wb") as local_loom_file:
        shutil.copyfileobj(response.raw, local_loom_file)

    ds = loompy.connect(local_loom_path)
    expression_sum = numpy.sum(ds[:, :])
    expression_nonzero = numpy.count_nonzero(ds[:, :])
    cell_count = ds.shape[1]

    return {"expression_sum": expression_sum, "expression_nonzero": expression_nonzero,
            "cell_count": cell_count}


def calculate_ss2_metrics_mtx(mtx_zip_url):
    """Calculate metrics for the mtx zip archive."""

    temp_dir = tempfile.mkdtemp(suffix="mtx_zip_test")
    local_mtx_zip_path = os.path.join(temp_dir, os.path.basename(mtx_zip_url))
    response = requests.get(mtx_zip_url, stream=True)
    with open(local_mtx_zip_path, "wb") as local_mtx_zip_file:
        shutil.copyfileobj(response.raw, local_mtx_zip_file)

    mtx_zip = zipfile.ZipFile(local_mtx_zip_path)
    mtx_name = [n for n in mtx_zip.namelist() if n.endswith("matrix.mtx")][0]

    matrix = scipy.io.mmread(io.BytesIO(mtx_zip.read(mtx_name)))

    return {
        "expression_sum": numpy.sum(matrix),
        "expression_nonzero": len(matrix.data),
        "cell_count": matrix.shape[1]
    }


def calculate_ss2_metrics_csv(csv_zip_url):
    """Calculte metrics for the zipped csv."""

    temp_dir = tempfile.mkdtemp(suffix="csv_zip_test")
    local_csv_zip_path = os.path.join(temp_dir, os.path.basename(csv_zip_url))
    response = requests.get(csv_zip_url, stream=True)
    with open(local_csv_zip_path, "wb") as local_csv_zip_file:
        shutil.copyfileobj(response.raw, local_csv_zip_file)

    csv_zip = zipfile.ZipFile(local_csv_zip_path)
    pdata = pandas.read_csv(
        io.StringIO(csv_zip.read(csv_zip.namelist()[0]).decode()),
        header=0,
        index_col=0)

    return {
        "expression_sum": numpy.sum(pdata.values),
        "expression_nonzero": numpy.count_nonzero(pdata.values),
        "cell_count": pdata.shape[0]
    }

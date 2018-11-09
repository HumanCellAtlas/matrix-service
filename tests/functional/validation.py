import concurrent.futures
import csv
import io
import os
import shutil
import tempfile

import hca.dss
import loompy
import numpy
import requests
import s3fs
import zarr


DSS_CLIENT = hca.dss.DSSClient()
DSS_STAGE = os.getenv('DSS_STAGE', "integration")
DSS_CLIENT.host = f"https://dss.{DSS_STAGE}.data.humancellatlas.org/v1"

S3 = s3fs.S3FileSystem(anon=True)


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
        bundle_expression_nonzero = 0
        for row in rsem_reader:
            bundle_expression_sum += float(row["TPM"])
            if float(row['TPM']) != 0.0:
                bundle_expression_nonzero += 1
        return {"expression_sum": bundle_expression_sum,
                "expression_nonzero": bundle_expression_nonzero,
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

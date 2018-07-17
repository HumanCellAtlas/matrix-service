import json
import os
import sys
import hca

from chalice.chalicelib.constants import SAMPLE_MATRICES_BUCKET_NAME
from config import BUNDLE_UUIDS_PATH


def upload_sample_matrices(dir_path):
    """
    Upload a bunch of matrices from a local directory to dss
    :param dir_path: Directory that contains directories of matrices
    """
    bundle_uuids = []
    client = hca.dss.DSSClient()

    for path in os.listdir(dir_path):
        file_path = os.path.join(dir_path, path)

        if not os.path.isdir(file_path):
            continue

        print("Uploading {} to DSS.".format(path))

        response = client.upload(src_dir=file_path, replica="aws", staging_bucket=SAMPLE_MATRICES_BUCKET_NAME)
        bundle_uuids.append(response["bundle_uuid"])

    # Store uuids for each uploaded directory into a bundle
    with open(BUNDLE_UUIDS_PATH, "w") as f:
        json.dump(bundle_uuids, f)

    print().info("Done.")


if __name__ == "__main__":
    upload_sample_matrices(sys.argv[1])

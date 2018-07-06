import os

# S3 Bucket for storing merged matrices
MERGED_MTX_BUCKET_NAME = "hca-dcp-matrix-service"

# S3 Bucket for storing matrices concatenation request status
MERGED_REQUEST_STATUS_BUCKET_NAME = "hca-dcp-matrix-service-request-status"

# S3 Bucket for staging sample matrices
STAGING_BUCKET_NAME = "matrix-service-test"

REQUEST_TEMPLATE = {
    "bundle_uuids": [],
    "status": "",
    "request_id": "",
    "merged_mtx_url": ""
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
BUNDLE_UUIDS_PATH = os.path.join(CONFIG_DIR, "bundle_uuids.json")

JSON_EXTENSION = ".json"

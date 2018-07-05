import os

# S3 Bucket for storing merged matrices
MERGED_MTX_BUCKET_NAME = "hca-dcp-matrix-service"

# S3 Bucket for storing matrices concatenation request status
MERGED_REQUEST_STATUS_BUCKET_NAME = "hca-dcp-matrix-service-request-status"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG_DIR = os.path.join(BASE_DIR, 'config')

REQUEST_TEMPLATE_PATH = os.path.join(CONFIG_DIR, 'request.json')
BUNDLE_UUIDS_PATH = os.path.join(CONFIG_DIR, "bundle_uuids.json")

JSON_EXTENSION = ".json"

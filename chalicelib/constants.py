import json
import os

from dcplib.aws_secret import AwsSecret

secret = AwsSecret(name="dcp/matrix-service/secrets")
secret_value = json.loads(secret.value)

# S3 Bucket for storing merged matrices
MERGED_MTX_BUCKET_NAME = secret_value['merged_mtx_bucket_name']

# S3 Bucket for storing matrices concatenation request status
REQUEST_STATUS_BUCKET_NAME = secret_value['request_status_bucket_name']

# S3 Bucket for staging sample matrices
SAMPLE_MATRICES_BUCKET_NAME = secret_value['sample_matrices_bucket_name']

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

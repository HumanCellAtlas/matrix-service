import json
import logging
import boto3
import hca

from dcplib.aws_secret import AwsSecret
from tweak import Config
from cloud_blobstore.s3 import S3BlobStore

# Default directory for all temp files
TEMP_DIR = "/tmp"

# Patch tweak package s.t it will write to tmp/ directory
Config._site_config_home = TEMP_DIR + Config._site_config_home
Config._user_config_home = TEMP_DIR + Config._user_config_home.split()[-1]

# HCA Client
hca_client = hca.dss.DSSClient()
hca_client.host = "https://dss.dev.data.humancellatlas.org/v1"

# Cloud_blobstore client
s3_blob_store = S3BlobStore(s3_client=boto3.client("s3"))

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load secret for the matrix service
secret = AwsSecret(name="dcp/matrix-service/secrets")
secret_value = json.loads(secret.value)

# S3 Bucket for storing merged matrices
MERGED_MTX_BUCKET_NAME = secret_value['merged_mtx_bucket_name']

# S3 Bucket for storing matrices concatenation request status
REQUEST_STATUS_BUCKET_NAME = secret_value['request_status_bucket_name']

# S3 Bucket for staging sample matrices
SAMPLE_MATRICES_BUCKET_NAME = secret_value['sample_matrices_bucket_name']

# SQS Queue for storing matrices concatenation requests
MS_SQS_QUEUE_NAME = secret_value['sqs_queue_name']

# Request template
REQUEST_TEMPLATE = {
    "bundle_uuids": [],
    "status": "",
    "request_id": "",
    "job_id": "",
    "merged_mtx_url": "",
    "time_spent_to_complete": "",
    "reason_to_abort": ""
}

# SQS Queue Message template
SQS_QUEUE_MSG = {
    "bundle_uuids": [],
    "job_id": ""
}

JSON_SUFFIX = ".json"

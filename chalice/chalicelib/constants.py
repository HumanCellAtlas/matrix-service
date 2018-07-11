import json

from dcplib.aws_secret import AwsSecret

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
    "merged_mtx_url": ""
}

# SQS Queue Message template
SQS_QUEUE_MSG = {
    "bundle_uuids": [],
    "job_id": ""
}

JSON_SUFFIX = ".json"

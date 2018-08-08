import json
import logging
import os
import boto3
import hca

from dcplib.aws_secret import AwsSecret
from tweak import Config
from cloud_blobstore.s3 import S3BlobStore

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Under dev stage, use the following hca client hostname
hca_client_host = "https://dss.dev.data.humancellatlas.org/v1"

ms_secret_name = "matrix-service/dev/terraform.tfvars"

# Default directory for all temp files
TEMP_DIR = "/tmp"

# Patch tweak package s.t it will write to tmp/ directory
Config._site_config_home = TEMP_DIR + Config._site_config_home
Config._user_config_home = TEMP_DIR + Config._user_config_home.split()[-1]

# HCA Client
hca_client = hca.dss.DSSClient()
hca_client.host = hca_client_host

# Cloud_blobstore client
s3_blob_store = S3BlobStore(s3_client=boto3.client("s3"))

# Faked DSS client
faked_dss_client = boto3.resource('s3').Bucket('matrix-service-faked-dss')

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load secret for the matrix service
secret = AwsSecret(name=ms_secret_name)
secret_value = json.loads(secret.value)

# S3 Bucket for storing merged matrices
MERGED_MTX_BUCKET_NAME = secret_value['hca_ms_merged_mtx_bucket']

# S3 Bucket for staging sample matrices
SAMPLE_MATRICES_BUCKET_NAME = secret_value['sample_matrices_bucket_name']

# SQS Queue for storing matrices concatenation requests
MS_SQS_QUEUE_NAME = secret_value['ms_sqs_queue']

# DynamoDB table for storing request status
REQUEST_STATUS_TABLE = secret_value['ms_dynamodb']

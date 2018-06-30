import hashlib
import json
import os
import tempfile
import boto3

from enum import Enum
from botocore.exceptions import ClientError
from chalicelib.constants import MERGED_REQUEST_STATUS_BUCKET_NAME, JSON_EXTENSION, REQUEST_TEMPLATE_PATH


class RequestStatus(Enum):
    UNINITIALIZED = "UNINITIALIZED"
    RUNNING = "RUNNING"
    DONE = "DONE"


class RequestHandler:
    @staticmethod
    def generate_request_id(bundle_uuids):
        """
        Generate a request id based on a list of bundle uuids.
        :param bundle_uuids: A list of bundle uuids.
        :return: Request id.
        """
        bundle_uuids.sort()
        m = hashlib.sha256()

        for bundle_uuid in bundle_uuids:
            m.update(bundle_uuid.encode())

        return m.hexdigest()

    @staticmethod
    def check_request_status(request_id):
        """
        Check the status of a matrix concatenation request.
        :param request_id: Matrices concatenation request id.
        :return: The status of the request.
        """
        s3 = boto3.resource("s3")
        key = request_id + JSON_EXTENSION

        try:
            response = s3.Object(bucket_name=MERGED_REQUEST_STATUS_BUCKET_NAME, key=key).get()
            body = json.loads(response['Body'].read())
            return RequestStatus(body["status"])

        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return RequestStatus("UNINITIALIZED")
            else:
                raise e

    @staticmethod
    def create_request(bundle_uuids, request_id):
        """
        Create a request status json file in s3 bucket.
        :param bundle_uuids: A list of bundle uuids.
        :param request_id: Matrices concatenation request id.
        """
        with open(REQUEST_TEMPLATE_PATH) as f:
            # Create a request based on a template json file
            request = json.load(f)
            request["bundle_uuids"] = bundle_uuids
            request["status"] = RequestStatus.RUNNING.name
            request["request_id"] = request_id

        # Create a temp file for storing the request
        fd, temp_file = tempfile.mkstemp(suffix=JSON_EXTENSION)

        with open(temp_file, "w") as f:
            json.dump(request, f)

        os.close(fd)

        # Key for request storing in s3 bucket
        key = request_id + JSON_EXTENSION

        s3 = boto3.resource("s3")
        s3.Object(bucket_name=MERGED_REQUEST_STATUS_BUCKET_NAME, key=key)\
            .put(Body=open(temp_file, "rb"))

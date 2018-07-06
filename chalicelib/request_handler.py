import hashlib
import json
import os
import tempfile
import boto3

from enum import Enum
from botocore.exceptions import ClientError
from chalicelib.constants import MERGED_REQUEST_STATUS_BUCKET_NAME, JSON_EXTENSION, \
    MERGED_MTX_BUCKET_NAME, REQUEST_TEMPLATE


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
    def update_request_status(bundle_uuids, request_id, status):
        """
        Update the request status json file in s3 bucket if exists. Otherwise,
        create a new status file.

        :param bundle_uuids: A list of bundle uuids.
        :param request_id: Matrices concatenation request id.
        :param status: Request status to update.
        """
        assert isinstance(status, RequestStatus)

        # Create a request based on a template dict
        request = REQUEST_TEMPLATE.copy()
        request["bundle_uuids"] = bundle_uuids
        request["status"] = status.name
        request["request_id"] = request_id

        if status == RequestStatus.DONE:
            # Key for merged matrix stored in s3 bucket
            key = request_id + ".loom"

            mtx_url = "s3://{}/{}".format(
                MERGED_MTX_BUCKET_NAME,
                key
            )
            request["merged_mtx_url"] = mtx_url

        _, temp_file = tempfile.mkstemp(suffix=JSON_EXTENSION)

        with open(temp_file, "w") as f:
            json.dump(request, f)

        # Key for request stored in s3 bucket
        key = request_id + JSON_EXTENSION

        s3 = boto3.resource("s3")
        s3.Object(bucket_name=MERGED_REQUEST_STATUS_BUCKET_NAME, key=key)\
            .put(Body=open(temp_file, "rb"))

        os.remove(temp_file)

import hashlib
import json
import boto3

from enum import Enum
from botocore.exceptions import ClientError
from chalicelib.constants import MERGED_REQUEST_STATUS_BUCKET_NAME


class RequestStatus(Enum):
    UNINITIALIZED = "UNINITIALIZED"
    RUNNING = "RUNNING"
    DONE = "DONE"


class RequestHandler:
    @staticmethod
    def generate_request_id(bundle_uuids):
        """
        Generate a request id based on a list of bundle uuids
        :param bundle_uuids: A list of bundle uuids
        :return: Request id
        """
        bundle_uuids.sort()
        m = hashlib.sha256()

        for bundle_uuid in bundle_uuids:
            m.update(bundle_uuid.encode())

        return m.hexdigest()

    @staticmethod
    def check_request_status(request_id):
        """
        Check the status of a matrix concatenation request
        :param request_id: Matrices concatenation request id
        :return: The status of the request
        """
        s3 = boto3.resource("s3")
        key = request_id + ".json"

        try:
            response = s3.Object(bucket_name=MERGED_REQUEST_STATUS_BUCKET_NAME, key=key).get()
            body = json.loads(response['Body'].read())
            return RequestStatus(body["status"])

        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return RequestStatus("UNINITIALIZED")
            else:
                raise e

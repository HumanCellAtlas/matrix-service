import hashlib
import json
import tempfile
import boto3

from enum import Enum
from typing import List
from cloud_blobstore import BlobNotFoundError
from chalicelib.config import REQUEST_STATUS_BUCKET_NAME, JSON_SUFFIX, \
    MERGED_MTX_BUCKET_NAME, REQUEST_TEMPLATE, TEMP_DIR, s3_blob_store


class RequestStatus(Enum):
    INITIALIZED = "INITIALIZED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    ABORT = "ABORT"


class RequestHandler:
    s3 = boto3.resource("s3")

    @staticmethod
    def generate_request_id(bundle_uuids: List[str]) -> str:
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
    def get_request_field(request_id: str, fieldname: str) -> str:
        """
        Get a field value from a request status file in s3.
        :param request_id: Matrices concatenation request id.
        :param fieldname: Field name in the request body
        :return: Corresponding field value in the request body.
        """
        key = request_id + JSON_SUFFIX

        try:
            content = s3_blob_store.get(bucket=REQUEST_STATUS_BUCKET_NAME, key=key)
            value = json.loads(content)[fieldname]
            return value
        except Exception as e:
            raise e

    @staticmethod
    def get_request_body(request_id: str) -> dict:
        """
        Get request body from a request status file in s3.
        :param request_id: Matrices concatenation request id.
        :return: Request body.
        """
        key = request_id + JSON_SUFFIX

        try:
            content = s3_blob_store.get(bucket=REQUEST_STATUS_BUCKET_NAME, key=key)
            body = json.loads(content)
            return body
        except Exception as e:
            raise e

    @staticmethod
    def update_request(
            bundle_uuids: List[str],
            request_id: str,
            job_id: str,
            status: RequestStatus,
            time_spent_to_complete="",
            reason_to_abort="") -> None:
        """
        Update the request status json file in s3 bucket if exists. Otherwise,
        create a new status file.

        :param bundle_uuids: A list of bundle uuids.
        :param request_id: Matrices concatenation request id.
        :param job_id: Matrices concatenation job id.
        :param status: Request status to update.
        :param time_spent_to_complete: Time spent to complete the concatenation job.
        :param reason_to_abort: Request abort reason.
        """
        request_key = request_id + JSON_SUFFIX

        # Check whether request status file exists in the s3 bucket; Create one if not.
        try:
            RequestHandler.get_request_body(request_id=request_id)
        except BlobNotFoundError:
            with tempfile.TemporaryFile(dir=TEMP_DIR, suffix=JSON_SUFFIX) as f:
                s3_blob_store.upload_file_handle(
                    bucket=REQUEST_STATUS_BUCKET_NAME,
                    key=request_key,
                    src_file_handle=f
                )
        except Exception as e:
            raise e

        # Create a request based on a template dict
        request = REQUEST_TEMPLATE.copy()
        request["bundle_uuids"] = bundle_uuids
        request["status"] = status.name
        request["request_id"] = request_id
        request["job_id"] = job_id
        request["time_spent_to_complete"] = time_spent_to_complete
        request["reason_to_abort"] = reason_to_abort

        if status == RequestStatus.DONE:
            # Key for merged matrix stored in s3 bucket
            mtx_key = request_id + ".loom"

            mtx_url = "s3://{}/{}".format(
                MERGED_MTX_BUCKET_NAME,
                mtx_key
            )
            request["merged_mtx_url"] = mtx_url

        try:
            s3_request_obj = RequestHandler.s3.Object(REQUEST_STATUS_BUCKET_NAME, request_key)
            s3_request_obj.put(Body=json.dumps(request))
        except Exception as e:
            raise e

import hashlib
import json
import os
import tempfile

from enum import Enum
from typing import Any, List
from cloud_blobstore import BlobNotFoundError, BlobStoreUnknownError
from chalicelib import s3_blob_store
from chalicelib.constants import REQUEST_STATUS_BUCKET_NAME, JSON_SUFFIX, \
    MERGED_MTX_BUCKET_NAME, REQUEST_TEMPLATE, TEMP_DIR


class RequestStatus(Enum):
    UNINITIALIZED = "UNINITIALIZED"
    INITIALIZED = "INITIALIZED"
    RUNNING = "RUNNING"
    DONE = "DONE"


class RequestHandler:
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
    def check_request_status(request_id: str) -> RequestStatus:
        """
        Check the status of a matrix concatenation request.
        :param request_id: Matrices concatenation request id.
        :return: The status of the request.
        """
        key = request_id + JSON_SUFFIX

        try:
            body = json.loads(s3_blob_store.get(bucket=REQUEST_STATUS_BUCKET_NAME, key=key))
            return RequestStatus(body["status"])
        except BlobNotFoundError:
            return RequestStatus("UNINITIALIZED")
        except BlobStoreUnknownError as e:
            raise e

    @staticmethod
    def update_request_status(
            bundle_uuids: List[str],
            request_id: str,
            job_id: str,
            status: RequestStatus) -> None:
        """
        Update the request status json file in s3 bucket if exists. Otherwise,
        create a new status file.

        :param bundle_uuids: A list of bundle uuids.
        :param request_id: Matrices concatenation request id.
        :param job_id: Matrices concatenation job id.
        :param status: Request status to update.
        """
        assert isinstance(status, RequestStatus)

        # Create a request based on a template dict
        request = REQUEST_TEMPLATE.copy()
        request["bundle_uuids"] = bundle_uuids
        request["status"] = status.name
        request["request_id"] = request_id
        request["job_id"] = job_id

        if status == RequestStatus.DONE:
            # Key for merged matrix stored in s3 bucket
            key = request_id + ".loom"

            mtx_url = "s3://{}/{}".format(
                MERGED_MTX_BUCKET_NAME,
                key
            )
            request["merged_mtx_url"] = mtx_url

        _, temp_file = tempfile.mkstemp(dir=TEMP_DIR, suffix=JSON_SUFFIX)

        with open(temp_file, "w") as f:
            json.dump(request, f)

        # Key for request stored in s3 bucket
        key = request_id + JSON_SUFFIX

        with open(temp_file, "rb") as f:
            s3_blob_store.upload_file_handle(
                bucket=REQUEST_STATUS_BUCKET_NAME,
                key=key,
                src_file_handle=f
            )

        os.remove(temp_file)

    @staticmethod
    def get_request_job_id(request_id: str):
        """
        Get job id from a request status file if exists.
        :param request_id: Matrix concatenation request id.
        :return: Job id if request exists. Otherwise, return None.
        """
        key = request_id + JSON_SUFFIX

        try:
            body = json.loads(s3_blob_store.get(bucket=REQUEST_STATUS_BUCKET_NAME, key=key))
            return body["job_id"]
        except BlobNotFoundError:
            return None
        except BlobStoreUnknownError as e:
            raise e

import json
import traceback
import unittest

from random import shuffle

from cloud_blobstore import BlobStoreUnknownError, BlobNotFoundError
from chalicelib import rand_uuid, s3_blob_store
from chalicelib.constants import JSON_SUFFIX, REQUEST_STATUS_BUCKET_NAME
from chalicelib.request_handler import RequestHandler, RequestStatus
from tests import rand_uuids


class TestRequestHandler(unittest.TestCase):
    def test_generate_request_id(self):
        """
        Request ids generated on uuids with different order should always
        be same.
        """
        bundle_uuids = rand_uuids(ub=11)
        bundle_uuids_copy = bundle_uuids.copy()
        shuffle(bundle_uuids_copy)

        # Make sure two list are in different order
        while bundle_uuids_copy == bundle_uuids:
            shuffle(bundle_uuids_copy)

        # Request ids generated for uuids in different order should be same
        self.assertEqual(
            RequestHandler.generate_request_id(bundle_uuids),
            RequestHandler.generate_request_id(bundle_uuids_copy)
        )

    def test_check_request_status(self):
        """
        Check status for an non-existing request should return UNINITIALIZED
        """
        non_existing_uuid = rand_uuid()

        try:
            status = RequestHandler.check_request_status(non_existing_uuid)
        except BlobStoreUnknownError:
            self.fail(traceback.format_exc())

        self.assertEqual(status, RequestStatus.UNINITIALIZED)

    def test_update_request_status(self):
        """
        Make sure update_request() function can successfully update the
        status json file stored in s3
        """
        bundle_uuids = rand_uuids(ub=11)
        request_id = RequestHandler.generate_request_id(bundle_uuids)
        status = RequestStatus.RUNNING
        job_id = rand_uuid()
        RequestHandler.update_request_status(
            bundle_uuids=bundle_uuids,
            request_id=request_id,
            job_id=job_id,
            status=status
        )

        # Load request status file(just uploaded) from s3
        key = request_id + JSON_SUFFIX

        try:
            body = json.loads(s3_blob_store.get(bucket=REQUEST_STATUS_BUCKET_NAME, key=key))
        except (BlobNotFoundError, BlobStoreUnknownError):
            self.fail(traceback.format_exc())

        self.assertEqual(bundle_uuids, body["bundle_uuids"])
        self.assertEqual(request_id, body["request_id"])
        self.assertEqual(job_id, body["job_id"])
        self.assertEqual(status.name, body["status"])

        # Merged matrix url for running request should be an empty string
        self.assertEqual(body["merged_mtx_url"], "")

        status = RequestStatus.DONE
        RequestHandler.update_request_status(
            bundle_uuids=bundle_uuids,
            request_id=request_id,
            job_id=job_id,
            status=status
        )

        # Reload latest request status json file from s3
        try:
            body = json.loads(s3_blob_store.get(bucket=REQUEST_STATUS_BUCKET_NAME, key=key))
        except (BlobNotFoundError, BlobStoreUnknownError):
            self.fail(traceback.format_exc())

        # Merged matrix url for done request should not be an empty string
        self.assertNotEqual(body["merged_mtx_url"], "")

        # Delete the s3 object after use
        s3_blob_store.delete(bucket=REQUEST_STATUS_BUCKET_NAME, key=key)


if __name__ == '__main__':
    unittest.main()

import json
import multiprocessing
import unittest
import boto3

from random import shuffle
from chalicelib import rand_uuid, rand_uuids
from chalicelib.constants import JSON_EXTENSION, MERGED_REQUEST_STATUS_BUCKET_NAME
from chalicelib.request_handler import RequestHandler, RequestStatus


class TestRequestHandler(unittest.TestCase):
    def test_generate_request_id(self):
        """
        Request ids generated on uuids with different order should always
        be same.
        """
        bundle_uuids = rand_uuids(10)
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
        status = RequestHandler.check_request_status(non_existing_uuid)
        self.assertEqual(status, RequestStatus.UNINITIALIZED)

    def test_update_request(self):
        """
        Make sure update_request() function can successfully update the
        status json file stored in s3
        """
        bundle_uuids = rand_uuids(10)
        request_id = RequestHandler.generate_request_id(bundle_uuids)
        status = RequestStatus.RUNNING.name
        RequestHandler.update_request(bundle_uuids, request_id, status)

        # Load request status file(just uploaded) from s3
        s3 = boto3.resource("s3")
        key = request_id + JSON_EXTENSION
        response = s3.Object(bucket_name=MERGED_REQUEST_STATUS_BUCKET_NAME, key=key).get()
        body = json.loads(response['Body'].read())

        self.assertEqual(bundle_uuids, body["bundle_uuids"])
        self.assertEqual(request_id, body["request_id"])
        self.assertEqual(status, body["status"])

        # Merged matrix url for running request should be an empty string
        self.assertEqual(body["merged_mtx_url"], "")

        status = RequestStatus.DONE.name
        RequestHandler.update_request(bundle_uuids, request_id, status)

        # Reload latest request status json file from s3
        response = s3.Object(bucket_name=MERGED_REQUEST_STATUS_BUCKET_NAME, key=key).get()
        body = json.loads(response['Body'].read())

        # Merged matrix url for done request should not be an empty string
        self.assertNotEqual(body["merged_mtx_url"], "")


if __name__ == '__main__':
    unittest.main()

import json
import traceback
import unittest

from chalicelib.config import s3_blob_store, REQUEST_STATUS_BUCKET_NAME, JSON_SUFFIX
from chalicelib.request_handler import RequestHandler, RequestStatus
from chalicelib.sqs import SqsQueueHandler
from cloud_blobstore import BlobNotFoundError, BlobStoreUnknownError
from tests import get_random_existing_bundle_uuids


class TestSqsQueueHandler(unittest.TestCase):
    def test_send_msg(self):
        """
        Make sure a msg can be correctly send to sqs queue.
        """

        # Get a random subset of bundle_uuids from sample bundle uuids
        bundle_uuids = get_random_existing_bundle_uuids(ub=5)
        request_id = RequestHandler.generate_request_id(bundle_uuids)
        key = request_id + JSON_SUFFIX

        # Send a msg
        msg_id = SqsQueueHandler.send_msg_to_ms_queue(bundle_uuids, request_id)

        # Make sure that request status is updated to INITIALIZED after msg is sent
        try:
            body = json.loads(s3_blob_store.get(bucket=REQUEST_STATUS_BUCKET_NAME, key=key))
            self.assertEqual(body["status"], RequestStatus.INITIALIZED.name)

            # Delete the s3 object after use
            s3_blob_store.delete(bucket=REQUEST_STATUS_BUCKET_NAME, key=key)
        except (BlobNotFoundError, BlobStoreUnknownError):
            self.fail(traceback.format_exc())

        # Check for msg existence in the sqs queue
        SqsQueueHandler.msg_exists_ms_queue(msg_id=msg_id)


if __name__ == '__main__':
    unittest.main()

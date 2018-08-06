import traceback
import unittest

from chalicelib.request_handler import RequestHandler, RequestStatus
from chalicelib.sqs import SqsQueueHandler
from tests import get_random_existing_bundle_uuids


class TestSqsQueueHandler(unittest.TestCase):
    def test_send_msg(self):
        """
        Make sure a msg can be correctly send to sqs queue.
        """

        # Get a random subset of bundle_uuids from sample bundle uuids
        bundle_uuids = get_random_existing_bundle_uuids(ub=5)
        request_id = RequestHandler.generate_request_id(bundle_uuids)

        # Send a msg
        msg_id = SqsQueueHandler.send_msg_to_ms_queue(bundle_uuids, request_id)

        # Make sure that request status is updated to INITIALIZED after msg is sent
        try:
            self.assertEqual(RequestHandler.get_request_status(request_id), RequestStatus.INITIALIZED.name)
            RequestHandler.delete_request(request_id)
        except:
            self.fail(traceback.format_exc())

        # Check for msg existence in the sqs queue
        SqsQueueHandler.msg_exists_ms_queue(msg_id=msg_id)


if __name__ == '__main__':
    unittest.main()
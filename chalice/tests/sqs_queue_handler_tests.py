import json
import unittest

from chalicelib import get_random_existing_bundle_uuids, ms_sqs_queue_msg_exists
from chalicelib.sqs_queue_handler import SqsQueueHandler


class TestSqsQueueHandler(unittest.TestCase):
    def test_send_msg(self):
        """
        Make sure a msg can be correctly send to sqs queue.
        """

        # Get a random subset of bundle_uuids from sample bundle uuids
        bundle_uuids_subset = get_random_existing_bundle_uuids(ub=5)

        # Send a msg
        msg_id = SqsQueueHandler.send_msg(json.dumps(bundle_uuids_subset))

        # Check for msg existence in the sqs queue
        ms_sqs_queue_msg_exists(msg_id=msg_id)


if __name__ == '__main__':
    unittest.main()

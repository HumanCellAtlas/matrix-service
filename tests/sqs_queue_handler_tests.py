import json
import unittest

from chalicelib.sqs import SqsQueueHandler
from tests import get_random_existing_bundle_uuids


class TestSqsQueueHandler(unittest.TestCase):
    def test_send_msg(self):
        """
        Make sure a msg can be correctly send to sqs queue.
        """

        # Get a random subset of bundle_uuids from sample bundle uuids
        bundle_uuids_subset = get_random_existing_bundle_uuids(ub=5)

        # Send a msg
        msg_id = SqsQueueHandler.send_msg_to_ms_queue(json.dumps(bundle_uuids_subset))

        # Check for msg existence in the sqs queue
        SqsQueueHandler.msg_exists_ms_queue(msg_id=msg_id)


if __name__ == '__main__':
    unittest.main()

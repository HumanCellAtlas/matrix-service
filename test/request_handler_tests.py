import unittest
from random import shuffle

from chalicelib import rand_uuid
from chalicelib.request_handler import RequestHandler


class TestRequestHandler(unittest.TestCase):
    def test_generate_request_id(self):
        """
        Request ids generated on uuids with different order should always
        be same.
        """
        bundle_uuids = []

        # Generate 10 random uuids
        for _ in range(10):
            bundle_uuids.append(rand_uuid())

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
        pass

    def test_update_request(self):
        pass


if __name__ == '__main__':
    unittest.main()

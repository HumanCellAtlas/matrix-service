import unittest
from random import shuffle

from chalicelib import rand_uuid
from chalicelib.request_handler import RequestHandler


class TestRequestHandler(unittest.TestCase):
    def test_generate_request_id(self):
        bundle_uuids = []

        # Generate 10 random uuids
        for _ in range(10):
            bundle_uuids.append(rand_uuid())

        # Request ids generated for uuids in different order should be same
        self.assertEqual(
            RequestHandler.generate_request_id(shuffle(bundle_uuids)),
            RequestHandler.generate_request_id(shuffle(bundle_uuids))
        )

    def test_check_request_status(self):
        pass

    def test_update_request(self):
        pass


if __name__ == '__main__':
    unittest.main()

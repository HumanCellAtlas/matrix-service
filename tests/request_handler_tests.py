import unittest

from random import shuffle
from chalicelib import rand_uuid
from chalicelib.request_handler import RequestStatus, RequestHandler
from tests import rand_uuids


class TestRequestHandler(unittest.TestCase):
    def test_generate_request_id(self):
        """
        Request ids generated on same uuids with different order should always
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

    def test_put_request(self):
        """
        Make sure put_request() function can successfully create/update the item in the request table.
        """
        bundle_uuids = rand_uuids(ub=11)
        request_id = RequestHandler.generate_request_id(bundle_uuids)
        status = RequestStatus.RUNNING
        job_id = rand_uuid()

        RequestHandler.put_request(
            bundle_uuids=bundle_uuids,
            request_id=request_id,
            job_id=job_id,
            status=status,
        )

        response = RequestHandler.get_request_attributes(request_id=request_id)

        self.assertEqual(bundle_uuids, response['bundle_uuids'])
        self.assertEqual(request_id, response['request_id'])
        self.assertEqual(job_id, response['job_id'])
        self.assertEqual(status.name, response['request_status'])
        self.assertEqual(response['reason_to_abort'], 'undefined')
        self.assertEqual(response['merged_mtx_url'], 'undefined')

        RequestHandler.delete_request(request_id)

    def test_get_request_attribute(self):
        """
        Make sure get_request_attribute() function can successfully get the corresponding attribute value
        of a request item.
        """
        bundle_uuids = rand_uuids(ub=11)
        request_id = RequestHandler.generate_request_id(bundle_uuids)
        status = RequestStatus.INITIALIZED
        job_id = rand_uuid()

        RequestHandler.put_request(
            bundle_uuids=bundle_uuids,
            request_id=request_id,
            job_id=job_id,
            status=status
        )

        self.assertEqual(bundle_uuids, RequestHandler.get_request_attribute(request_id, 'bundle_uuids'))
        self.assertEqual(request_id, RequestHandler.get_request_attribute(request_id, 'request_id'))
        self.assertEqual(status.name, RequestHandler.get_request_status(request_id))
        self.assertEqual(job_id, RequestHandler.get_request_job_id(request_id))
        self.assertEqual(RequestHandler.get_request_attribute(request_id, 'merged_mtx_url'), 'undefined')
        self.assertEqual(RequestHandler.get_request_attribute(request_id, "reason_to_abort"), 'undefined')

        RequestHandler.delete_request(request_id)


if __name__ == '__main__':
    unittest.main()

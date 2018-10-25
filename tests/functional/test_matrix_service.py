import json
import os
import requests
import time
import unittest

import s3fs
import zarr


from . import validation
from .wait_for import WaitFor
from matrix.common.constants import MatrixRequestStatus


INPUT_BUNDLE_IDS = {
    "dev": [
        "ca8308ee-8388-44d6-b4c3-dea6da1334f1.2018-10-18T001232.039329Z",
        "8a82f068-d324-428c-82ad-14def15442c3.2018-10-17T235815.275412Z",
        "3b196d5e-7d88-4d02-9c6f-61271764e4ba.2018-10-17T235315.092228Z",
        "bcd855b6-e39a-4f55-a2be-9c0d094510e2.2018-10-17T234649.368503Z",
        "b51f5de3-4d65-484a-bd16-7e0bb1a6df59.2018-10-17T234148.910728Z",
    ],
    "integration": [
        "0f997914-43c2-45e2-b79f-99167295b263.2018-10-17T204940.626010Z",
        "167a2b69-f52f-4a0a-9691-d1db62ef12de.2018-10-17T201019.320177Z",
        "b2965ca9-4aca-4baf-9606-215508d1e475.2018-10-17T200207.329078Z",
        "8d567bed-a9aa-4a39-9467-75510b965257.2018-10-17T191234.528671Z",
        "ba9c63ac-6db5-48bc-a2e3-7be4ddd03d97.2018-10-17T173508.111787Z",
    ],
    "staging": [
        "9cc50869-2a7e-4740-80ca-6afba34e5b7f.2018-10-18T155534.401810Z",
        "799e7a7a-2b86-41fb-9382-7100ca4edd1b.2018-10-18T160919.242128Z",
        "5b8a7ea4-9911-4529-89c7-4ba36044322d.2018-10-18T161249.988019Z",
        "6a362d71-c6df-4797-9fa8-686b7e2d406c.2018-10-18T161937.774601Z",
        "39afcc07-2b14-4844-a459-9c1c29dc676f.2018-10-18T163113.248957Z",
    ]
}


class TestMatrixService(unittest.TestCase):

    def setUp(self):
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']
        self.api_url = f"https://{os.environ['API_HOST']}/v0"
        self.res_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "res")
        self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        self.verbose = True
        self.s3_file_system = s3fs.S3FileSystem(anon=False)

    def test_zarr_output_matrix_service(self):
        request_id = self._post_matrix_service_request(
            INPUT_BUNDLE_IDS[self.deployment_stage], "zarr")
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=180)
        self._analyze_zarr_matrix_results(request_id, INPUT_BUNDLE_IDS[self.deployment_stage])

    def test_loom_output_matrix_service(self):
        request_id = self._post_matrix_service_request(
            INPUT_BUNDLE_IDS[self.deployment_stage], "loom")
        # timeout seconds is increased to 600 as batch may tak time to spin up spot instances for conversion.
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=180)
        self._analyze_loom_matrix_results(request_id, INPUT_BUNDLE_IDS[self.deployment_stage])

    @unittest.skip
    def test_matrix_service_ss2_small(self):
        # make request and receive job id back
        request_id = self._post_matrix_service_request(
            INPUT_BUNDLE_IDS[self.deployment_stage], "zarr")

        # wait for post to complete
        time.sleep(2)
        # wait for get requests to return 200 and status of COMPLETED
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=600)
        self._analyze_loom_matrix_results(request_id, INPUT_BUNDLE_IDS[self.deployment_stage])

    def test_matrix_service_without_specified_output(self):
        request_id = request_id = self._post_matrix_service_request(
            INPUT_BUNDLE_IDS[self.deployment_stage])
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=180)
        self._analyze_zarr_matrix_results(request_id, INPUT_BUNDLE_IDS[self.deployment_stage])

    def test_matrix_service_ss2(self):
        timeout = int(os.getenv("MATRIX_TEST_TIMEOUT", 300))
        num_bundles = int(os.getenv("MATRIX_TEST_NUM_BUNDLES", 105))
        bundle_fqids = json.loads(open(f"{self.res_dir}/pancreas_ss2_2544_demo_bundles.json", "r").read())[:num_bundles]

        request_id = self._post_matrix_service_request(bundle_fqids, "zarr")

        # wait for request to complete
        time.sleep(2)
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=timeout)

        self._verify_row_counts(request_id, num_bundles)

    def _post_matrix_service_request(self, bundle_fqids, format=None):
        data = {
            'bundle_fqids': bundle_fqids,
            'format': format
        }
        if format:
            data["format"] = format
        response = self._make_request(description="POST REQUEST TO MATRIX SERVICE",
                                      verb='POST',
                                      url=f"{self.api_url}/matrix",
                                      expected_status=202,
                                      data=json.dumps(data),
                                      headers=self.headers)
        data = json.loads(response)
        # Need a sleep since the driver creates entry in state table.
        # Driver's execution may occur after completion of post request.
        # Fix by adding entry to state table directly in post request.
        time.sleep(2)
        return data["request_id"]

    def _poll_get_matrix_service_request(self, request_id):
        url = f"{self.api_url}/matrix/{request_id}"
        response = self._make_request(description="GET REQUEST TO MATRIX SERVICE WITH REQUEST ID",
                                      verb='GET',
                                      url=url,
                                      expected_status=200,
                                      headers=self.headers)
        data = json.loads(response)
        status = data["status"]
        return status

    def _verify_row_counts(self, request_id, expected_num_rows):
        """Verify that the output matrix has the expected number of rows."""
        matrix_location = self._retrieve_matrix_location(request_id)
        store = s3fs.S3Map(matrix_location, s3=self.s3_file_system, check=False, create=False)
        group = zarr.group(store=store)

        self.assertEqual(group.expression.shape[0], expected_num_rows)
        self.assertEqual(group.cell_metadata_numeric.shape[0], expected_num_rows)
        self.assertEqual(group.cell_metadata_string.shape[0], expected_num_rows)
        self.assertEqual(group.cell_id.shape[0], expected_num_rows)

    def _analyze_zarr_matrix_results(self, request_id, input_bundles):

        direct_metrics = validation.calculate_ss2_metrics_direct(input_bundles)

        matrix_location = self._retrieve_matrix_location(request_id)
        self.assertEqual(matrix_location.endswith("zarr"), True)

        zarr_metrics = validation.calculate_ss2_metrics_zarr(matrix_location)

        for metric in zarr_metrics:
            self.assertEqual(zarr_metrics[metric], direct_metrics[metric])

    def _analyze_loom_matrix_results(self, request_id, input_bundles):
        direct_metrics = validation.calculate_ss2_metrics_direct(input_bundles)

        matrix_location = self._retrieve_matrix_location(request_id)
        self.assertEqual(matrix_location.endswith("loom"), True)
        loom_metrics = validation.calculate_ss2_metrics_loom(matrix_location)

        for metric in loom_metrics:
            self.assertEqual(loom_metrics[metric], direct_metrics[metric])

    def _retrieve_matrix_location(self, request_id):
        url = f"{self.api_url}/matrix/{request_id}"
        response = self._make_request(description="GET REQUEST TO MATRIX SERVICE WITH REQUEST ID",
                                      verb='GET',
                                      url=url,
                                      expected_status=200,
                                      headers=self.headers)
        data = json.loads(response)
        location = data["matrix_location"]
        return location

    def _make_request(self, description, verb, url, expected_status=None, **options):
        print(description + ": ")
        print(f"{verb.upper()} {url}")

        method = getattr(requests, verb.lower())
        response = method(url, **options)

        print(f"-> {response.status_code}")
        if expected_status:
            self.assertEqual(expected_status, response.status_code)

        if response.content:
            print(response.content.decode('utf8'))

        return response.content

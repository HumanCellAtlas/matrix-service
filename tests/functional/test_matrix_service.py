import json
import os
import time
import unittest

import requests
import s3fs

from . import validation
from .wait_for import WaitFor
from matrix.common.constants import MatrixRequestStatus


INPUT_BUNDLE_IDS = {
    "dev": [
        # matrix-service dev points to DSS integration.
        # These bundles exist in DSS integration.
        "0f997914-43c2-45e2-b79f-99167295b263.2018-10-17T204940.626010Z",
        "167a2b69-f52f-4a0a-9691-d1db62ef12de.2018-10-17T201019.320177Z",
        "b2965ca9-4aca-4baf-9606-215508d1e475.2018-10-17T200207.329078Z",
        "8d567bed-a9aa-4a39-9467-75510b965257.2018-10-17T191234.528671Z",
        "ba9c63ac-6db5-48bc-a2e3-7be4ddd03d97.2018-10-17T173508.111787Z",
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

INPUT_BUNDLE_URL = \
    "https://s3.amazonaws.com/dcp-matrix-test-data/{deployment_stage}_test_bundles.tsv"


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
            bundle_fqids=INPUT_BUNDLE_IDS[self.deployment_stage], format="zarr")
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=300)
        self._analyze_zarr_matrix_results(request_id, INPUT_BUNDLE_IDS[self.deployment_stage])

    def test_loom_output_matrix_service(self):
        request_id = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.deployment_stage], format="loom")
        # timeout seconds is increased to 600 as batch may tak time to spin up spot instances for conversion.
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=600)
        self._analyze_loom_matrix_results(request_id, INPUT_BUNDLE_IDS[self.deployment_stage])

    def test_matrix_service_without_specified_output(self):
        request_id = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.deployment_stage])
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=300)
        self._analyze_zarr_matrix_results(request_id, INPUT_BUNDLE_IDS[self.deployment_stage])

    def test_matrix_service_invalid_bundle(self):
        test_bundle_uuids = ["bundle1.version", "bundle2.version"]
        request_id = self._post_matrix_service_request(bundle_fqids=test_bundle_uuids, format="loom")
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.FAILED.value, timeout_seconds=60)

    def test_matrix_service_bundle_not_found(self):
        test_bundle_uuids = ["00000000-0000-0000-0000-000000000000.version"]
        request_id = self._post_matrix_service_request(bundle_fqids=test_bundle_uuids, format="loom")
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.FAILED.value, timeout_seconds=60)

    @unittest.skipUnless(os.getenv('DEPLOYMENT_STAGE') == "staging",
                         "SS2 Pancreas bundles are only available in staging.")
    def test_matrix_service_ss2(self):
        timeout = int(os.getenv("MATRIX_TEST_TIMEOUT", 300))
        num_bundles = int(os.getenv("MATRIX_TEST_NUM_BUNDLES", 200))
        bundle_fqids = json.loads(open(f"{self.res_dir}/pancreas_ss2_2544_demo_bundles.json", "r").read())[:num_bundles]

        request_id = self._post_matrix_service_request(bundle_fqids=bundle_fqids, format="zarr")

        # wait for request to complete
        time.sleep(2)
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=timeout)

        self._analyze_zarr_matrix_results(request_id, bundle_fqids)

    def test_bundle_url(self):
        timeout = int(os.getenv("MATRIX_TEST_TIMEOUT", 300))
        bundle_fqids_url = INPUT_BUNDLE_URL.format(deployment_stage=self.deployment_stage)

        request_id = self._post_matrix_service_request(
            bundle_fqids_url=bundle_fqids_url,
            format="zarr")

        # wait for request to complete
        time.sleep(2)
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=timeout)
        bundle_fqids = ['.'.join(el.split('\t')) for el in
                        requests.get(bundle_fqids_url).text.strip().split('\n')[1:]]

        self._analyze_zarr_matrix_results(request_id, bundle_fqids)

    def _post_matrix_service_request(self, bundle_fqids=None, bundle_fqids_url=None, format=None):
        data = {}
        if bundle_fqids:
            data["bundle_fqids"] = bundle_fqids
        if bundle_fqids_url:
            data["bundle_fqids_url"] = bundle_fqids_url
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

    def _analyze_zarr_matrix_results(self, request_id, input_bundles):

        direct_metrics = validation.calculate_ss2_metrics_direct(input_bundles)

        matrix_location = self._retrieve_matrix_location(request_id)

        self.assertEqual(matrix_location.endswith("zarr"), True)

        zarr_metrics = validation.calculate_ss2_metrics_zarr(matrix_location)
        self._compare_metrics(direct_metrics, zarr_metrics)

    def _analyze_loom_matrix_results(self, request_id, input_bundles):
        direct_metrics = validation.calculate_ss2_metrics_direct(input_bundles)

        matrix_location = self._retrieve_matrix_location(request_id)
        self.assertEqual(matrix_location.endswith("loom"), True)
        loom_metrics = validation.calculate_ss2_metrics_loom(matrix_location)
        self._compare_metrics(direct_metrics, loom_metrics)

    def _compare_metrics(self, metrics_1, metrics_2):
        for metric in metrics_1:
            delta = metrics_1[metric] / 100000
            self.assertAlmostEqual(
                metrics_1[metric], metrics_2[metric], delta=delta,
                msg=(f"Metric {metric} doesn't match: {metrics_1[metric]} "
                     f"{metrics_2[metric]}"))

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

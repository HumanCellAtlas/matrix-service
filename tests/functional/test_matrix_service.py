import json
import os
import time
import unittest

import requests
import s3fs

from . import validation
from .wait_for import WaitFor
from matrix.common.constants import MatrixRequestStatus
from matrix.common.aws.dynamo_handler import (CacheTableField, DynamoHandler,
                                              OutputTableField, StateTableField)
from matrix.common.request.request_cache import RequestCache


MATRIX_ENV_TO_DSS_ENV = {
    'predev': "integration",
    'dev': "integration",
    'integration': "integration",
    'staging': "staging",
    'prod': "prod",
}

INPUT_BUNDLE_IDS = {
    "integration": [
        "5cb665f4-97bb-4176-8ec2-1b83b95c1bc0.2019-02-11T171739.925160Z",
        "ff7ef351-f46f-4c39-b4c3-c8b33423a4c9.2019-02-11T124842.494942Z",
        "aa8262c2-7a0e-49fd-bac1-d41a4019bd87.2019-02-10T234926.510991Z",
        "0ef88e4a-a779-4588-8677-953d65ca6d9a.2019-02-10T124405.139571Z",
        "c881020e-9f53-4f7e-9c49-d9dbd9e8f280.2019-02-09T124912.755814Z",
    ],
    "staging": [
        "17ff4cc3-1875-49b7-8c7c-eaa6e1a6f6b7.2019-02-14T201954.486100Z",
        "eafbc11c-ae37-4ee2-9de1-a85d2b12636e.2019-02-14T201958.686240Z",
        "979ab20d-d070-48f9-adf0-5062ca8414bb.2019-02-14T195727.953202Z",
        "ac5d80e2-5876-44e8-b1b8-e3b70d0a617a.2019-02-14T192300.024539Z",
        "02f6c9d6-2cdc-43bf-86c4-fedf3b35d6af.2019-02-14T182414.491107Z",
    ],
    "prod": [
        "0552e6b3-ee09-425e-adbb-01fb9467e6f3.2018-11-06T231250.330922Z",
        "79e14c18-7bf7-4883-92a1-b7b26c3067d4.2018-11-06T232117.884033Z",
        "1134eb98-e7e9-45af-b2d8-b9886b633929.2018-11-06T231738.177145Z",
        "1d6de514-115f-4ed6-8d11-22ad02e394bc.2018-11-06T231755.376078Z",
        "2ec96e8c-6d28-4d98-9a19-7c95bbe13ce2.2018-11-06T231809.821560Z",
    ]
}

INPUT_BUNDLE_URL = \
    "https://s3.amazonaws.com/dcp-matrix-test-data/{dss_env}_test_bundles.tsv"


class TestMatrixService(unittest.TestCase):

    def setUp(self):
        self.dss_env = MATRIX_ENV_TO_DSS_ENV[os.environ['DEPLOYMENT_STAGE']]
        self.api_url = f"https://{os.environ['API_HOST']}/v0"
        self.res_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "res")
        self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        self.verbose = True
        self.s3_file_system = s3fs.S3FileSystem(anon=False)

    def tearDown(self):
        """Try to remove any table entries created for the tests, especially entries
        will result in future tests returning cached results.
        """
        dynamo_handler = DynamoHandler()
        request_hash = RequestCache(self.request_id).retrieve_hash()

        print(dynamo_handler._state_table.delete_item(
            Key={StateTableField.REQUEST_HASH.value: request_hash}
        ))

        print(dynamo_handler._output_table.delete_item(
            Key={OutputTableField.REQUEST_HASH.value: request_hash}
        ))

        print(dynamo_handler._cache_table.delete_item(
            Key={CacheTableField.REQUEST_ID.value: self.request_id}
        ))

    def test_zarr_output_matrix_service(self):
        self.request_id = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env], format="zarr")
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=300)
        self._analyze_zarr_matrix_results(self.request_id, INPUT_BUNDLE_IDS[self.dss_env])

    def test_loom_output_matrix_service(self):
        self.request_id = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env], format="loom")
        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=1200)
        self._analyze_loom_matrix_results(self.request_id, INPUT_BUNDLE_IDS[self.dss_env])

    def test_csv_output_matrix_service(self):
        self.request_id = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env], format="csv")
        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_get_matrix_service_request, self.request_id) \
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=1200)
        self._analyze_csv_matrix_results(self.request_id, INPUT_BUNDLE_IDS[self.dss_env])

    def test_mtx_output_matrix_service(self):
        self.request_id = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env], format="mtx")
        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_get_matrix_service_request, self.request_id) \
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=1200)
        self._analyze_mtx_matrix_results(self.request_id, INPUT_BUNDLE_IDS[self.dss_env])

    def test_matrix_service_without_specified_output(self):
        self.request_id = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env])
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=300)
        self._analyze_zarr_matrix_results(self.request_id, INPUT_BUNDLE_IDS[self.dss_env])

    def test_matrix_service_invalid_bundle(self):
        test_bundle_uuids = ["bundle1.version", "bundle2.version"]
        self.request_id = self._post_matrix_service_request(
            bundle_fqids=test_bundle_uuids, format="loom")
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.FAILED.value, timeout_seconds=60)

    def test_matrix_service_bundle_not_found(self):
        test_bundle_uuids = ["00000000-0000-0000-0000-000000000000.version"]
        self.request_id = self._post_matrix_service_request(
            bundle_fqids=test_bundle_uuids, format="loom")
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.FAILED.value, timeout_seconds=60)

    @unittest.skipUnless(os.getenv('DEPLOYMENT_STAGE') == "staging",
                         "SS2 Pancreas bundles are only available in staging.")
    def test_matrix_service_ss2(self):
        timeout = int(os.getenv("MATRIX_TEST_TIMEOUT", 300))
        num_bundles = int(os.getenv("MATRIX_TEST_NUM_BUNDLES", 200))
        bundle_fqids = json.loads(open(f"{self.res_dir}/pancreas_ss2_2544_demo_bundles.json", "r").read())[:num_bundles]

        self.request_id = self._post_matrix_service_request(
            bundle_fqids=bundle_fqids, format="zarr")

        # wait for request to complete
        time.sleep(2)
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=timeout)

        self._analyze_zarr_matrix_results(self.request_id, bundle_fqids)

    def test_bundle_url(self):
        timeout = int(os.getenv("MATRIX_TEST_TIMEOUT", 300))
        bundle_fqids_url = INPUT_BUNDLE_URL.format(dss_env=self.dss_env)

        self.request_id = self._post_matrix_service_request(
            bundle_fqids_url=bundle_fqids_url,
            format="zarr")

        # wait for request to complete
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=timeout)
        bundle_fqids = ['.'.join(el.split('\t')) for el in
                        requests.get(bundle_fqids_url).text.strip().split('\n')[1:]]

        self._analyze_zarr_matrix_results(self.request_id, bundle_fqids)

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

    def _analyze_mtx_matrix_results(self, request_id, input_bundles):
        direct_metrics = validation.calculate_ss2_metrics_direct(input_bundles)

        matrix_location = self._retrieve_matrix_location(request_id)
        self.assertEqual(matrix_location.endswith("mtx.zip"), True)
        mtx_metrics = validation.calculate_ss2_metrics_mtx(matrix_location)
        self._compare_metrics(direct_metrics, mtx_metrics)

    def _analyze_csv_matrix_results(self, request_id, input_bundles):
        direct_metrics = validation.calculate_ss2_metrics_direct(input_bundles)

        matrix_location = self._retrieve_matrix_location(request_id)
        self.assertEqual(matrix_location.endswith("csv.zip"), True)
        csv_metrics = validation.calculate_ss2_metrics_csv(matrix_location)
        self._compare_metrics(direct_metrics, csv_metrics)

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

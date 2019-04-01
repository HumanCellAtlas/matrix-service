import json
import os
import time
import unittest

import requests
import s3fs

from . import validation
from .wait_for import WaitFor
from matrix.common.constants import MatrixRequestStatus
from matrix.common.aws.redshift_handler import RedshiftHandler


MATRIX_ENV_TO_DSS_ENV = {
    'predev': "integration",
    'dev': "prod",
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
        "4d825362-dc29-4135-9f53-92c27955ddda.2019-02-02T072347.000888Z",
        "ae066cbd-f9f8-438a-8c3c-43f5cd44a9eb.2019-02-02T042049.843007Z",
        "684a7318-a405-450b-a616-f97ddbc34f8f.2019-02-01T183026.372619Z",
        "e83d8b22-2090-4f34-868d-92a8749a401d.2019-02-02T021732.127000Z",
        "2c1160e9-2324-4dfa-9776-d93f76d31fde.2018-11-21T140133.660897Z",
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
        self.redshift_handler = RedshiftHandler()

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
        self._analyze_loom_matrix_results(self.request_id, INPUT_BUNDLE_IDS[self.dss_env])

    @unittest.skipUnless(os.getenv('DEPLOYMENT_STAGE') != "prod",
                         "Do not want to process fake notifications in production.")
    def test_dss_notification(self):
        bundle_fqid = INPUT_BUNDLE_IDS[self.dss_env][0]

        self._post_notification(bundle_fqid=bundle_fqid, event_type="DELETE")
        WaitFor(self._poll_db_get_analysis_row_count_from_fqid, bundle_fqid)\
            .to_return_value(0, timeout_seconds=60)

        self._post_notification(bundle_fqid=bundle_fqid, event_type="CREATE")
        WaitFor(self._poll_db_get_analysis_row_count_from_fqid, bundle_fqid)\
            .to_return_value(1, timeout_seconds=600)

        self._post_notification(bundle_fqid=bundle_fqid, event_type="TOMBSTONE")
        WaitFor(self._poll_db_get_analysis_row_count_from_fqid, bundle_fqid)\
            .to_return_value(0, timeout_seconds=60)

        self._post_notification(bundle_fqid=bundle_fqid, event_type="CREATE")
        WaitFor(self._poll_db_get_analysis_row_count_from_fqid, bundle_fqid)\
            .to_return_value(1, timeout_seconds=600)

    @unittest.skipUnless(os.getenv('DEPLOYMENT_STAGE') == "staging",
                         "SS2 Pancreas bundles are only available in staging.")
    def test_matrix_service_ss2(self):
        timeout = int(os.getenv("MATRIX_TEST_TIMEOUT", 300))
        num_bundles = int(os.getenv("MATRIX_TEST_NUM_BUNDLES", 200))
        bundle_fqids = json.loads(open(f"{self.res_dir}/pancreas_ss2_2544_demo_bundles.json", "r").read())[:num_bundles]

        self.request_id = self._post_matrix_service_request(
            bundle_fqids=bundle_fqids, format="loom")

        # wait for request to complete
        time.sleep(2)
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=timeout)

        self._analyze_loom_matrix_results(self.request_id, bundle_fqids)

    def test_bundle_url(self):
        timeout = int(os.getenv("MATRIX_TEST_TIMEOUT", 300))
        bundle_fqids_url = INPUT_BUNDLE_URL.format(dss_env=self.dss_env)

        self.request_id = self._post_matrix_service_request(
            bundle_fqids_url=bundle_fqids_url,
            format="loom")

        # wait for request to complete
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=timeout)
        bundle_fqids = ['.'.join(el.split('\t')) for el in
                        requests.get(bundle_fqids_url).text.strip().split('\n')[1:]]

        self._analyze_loom_matrix_results(self.request_id, bundle_fqids)

    def _poll_db_get_analysis_row_count_from_fqid(self, bundle_fqid):
        query = f"SELECT count(*) from analysis where analysis.bundle_fqid = '{bundle_fqid}'"
        results = self.redshift_handler.transaction([query], return_results=True)
        count = results[0][0]
        return count

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

    def _post_notification(self, bundle_fqid, event_type):
        data = {}
        bundle_uuid = bundle_fqid.split('.', 1)[0]
        bundle_version = bundle_fqid.split('.', 1)[1]

        data["transaction_id"] = "test_transaction_id"
        data["subscription_id"] = "test_subscription_id"
        data["event_type"] = event_type
        data["match"] = {}
        data["match"]["bundle_uuid"] = bundle_uuid
        data["match"]["bundle_version"] = bundle_version

        response = self._make_request(description="POST NOTIFICATION TO MATRIX SERVICE",
                                      verb='POST',
                                      url=f"{self.api_url}/dss/notification",
                                      expected_status=200,
                                      data=json.dumps(data),
                                      headers=self.headers)
        data = json.loads(response)
        return data

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

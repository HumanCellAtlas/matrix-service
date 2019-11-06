import io
import json
import os
import pandas
import shutil
import tempfile
import time
import unittest
import zipfile

import loompy
import requests
import s3fs
from requests_http_signature import HTTPSignatureAuth

from . import validation
from .wait_for import WaitFor
from matrix.common.config import MatrixInfraConfig
from matrix.common.constants import (GenusSpecies, MATRIX_ENV_TO_DSS_ENV, MatrixRequestStatus,
                                     DEFAULT_FIELDS, DEFAULT_FEATURE)
from matrix.common.aws.redshift_handler import RedshiftHandler
from matrix.common.query_constructor import format_str_list
from scripts.dss_subscription import DSS_SUBSCRIPTION_HMAC_SECRET_ID


INPUT_BUNDLE_IDS = {
    "integration": [
        "93cb1d90-10f3-4b76-aa6a-a85450a83968.2019-11-06T140752.790454Z",
        "626d3e17-6a62-4fe7-838a-323f2cec450a.2019-11-06T140922.234528Z",
        "4602ed62-8dc1-4b67-a50d-35c3ab34b787.2019-11-06T141032.303970Z",
        "ce51949d-ca8f-4194-8f96-e470dd749dfa.2019-11-06T143122.339686Z",
        "635b4815-0cba-4285-b66d-e88b97bf3f20.2019-11-06T141546.990073Z"
    ],
    "staging": [
        "d30147bb-5fa2-4444-ad62-398308e4f8d3.2019-07-18T051541.009170Z",
        "d9c742f0-b131-4e06-86d6-990f940c98a2.2019-05-24T220602.109000Z",
        "d63e36c0-ac2d-484d-9fd9-a111ebf8231a.2019-07-19T041938.545669Z",
        "9c8c5ba1-27fb-496f-9dfe-b605a9aa9658.2018-10-24T232635.721854Z",
        "7ba67071-6fb3-43f8-ada8-ed5993195e2b.2018-10-24T225455.712647Z",
    ],
    "prod": [
        "ffd3bc7b-8f3b-4f97-aa2a-78f9bac93775.2019-05-14T122736.345000Z",
        "f69b288c-fabc-4ac8-b50c-7abcae3731bc.2019-05-14T120110.781000Z",
        "f8ba80a9-71b1-4c15-bcfc-c05a50660898.2019-05-14T122536.545000Z",
        "fd202a54-7085-406d-a92a-aad6dd2d3ef0.2019-05-14T121656.910000Z",
        "fffe55c1-18ed-401b-aa9a-6f64d0b93fec.2019-05-17T233932.932000Z",
    ]
}

NOTIFICATION_TEST_DATA = {
    "integration": {
        'bundle_fqid': "ce51949d-ca8f-4194-8f96-e470dd749dfa.2019-11-06T143122.339686Z",
        'cell_count': 1,
        'exp_count': 21876
    },
    "staging": {
        'bundle_fqid': "119f6f39-d111-4c33-a3d5-224a67655b07.2018-10-24T224220.927365Z",
        'cell_count': 1,
        'exp_count': 21680
    },
    # notification test does not run on prod, however other matrix environments may point to dss prod
    "prod": {
        'bundle_fqid': "ffc82dff-2490-409a-8519-98d6e8bd9a9b.2019-05-14T161401.716000Z",
        'cell_count': 1,
        'exp_count': 39542
    }
}

MOUSE_BUNDLE_IDS = {
    "integration": [
        "e20d94ad-e061-40b8-a600-b299dd2b9f27.2019-08-01T005511.532305Z"
    ],
    "staging": [
    ],
    "prod": [
        "e20d94ad-e061-40b8-a600-b299dd2b9f27.2019-10-04T094331.797559Z"
    ]
}

INPUT_BUNDLE_URL = \
    "https://s3.amazonaws.com/dcp-matrix-test-data/{dss_env}_test_bundles.tsv"


class MatrixServiceTest(unittest.TestCase):

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

    def _poll_all_requests_in_status(self, request_ids, target_status):
        return all(self._poll_get_matrix_service_request(req) == target_status
                   for req in request_ids.values())

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

    def _cleanup_matrix_result(self, request_id):
        s3_file_system = s3fs.S3FileSystem(anon=False)

        matrix_location = self._retrieve_matrix_location(request_id)
        results_bucket = os.environ['MATRIX_RESULTS_BUCKET']
        if matrix_location.find(results_bucket) > -1:
            s3_key = matrix_location[matrix_location.find(results_bucket):]
            s3_file_system.rm(s3_key)

    def _cleanup_all_matrix_results(self, request_ids):
        for request_id in request_ids.values():
            self._cleanup_matrix_result(request_id)

        self.request_ids = None

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
        try:
            location = data["matrix_location"]
        except KeyError:
            location = data["matrix_url"]
        return location


class TestMatrixServiceV0(MatrixServiceTest):

    def setUp(self):
        self.dss_env = MATRIX_ENV_TO_DSS_ENV[os.environ['DEPLOYMENT_STAGE']]
        self.api_url = f"https://{os.environ['API_HOST']}/v0"
        self.res_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "res")
        self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        self.verbose = True
        self.s3_file_system = s3fs.S3FileSystem(anon=False)
        self.redshift_handler = RedshiftHandler()

    def tearDown(self):
        if hasattr(self, 'request_ids') and self.request_ids:
            self._cleanup_all_matrix_results(self.request_ids)

    def test_single_bundle_request(self):
        self.request_ids = self._post_matrix_service_request(
            bundle_fqids=[INPUT_BUNDLE_IDS[self.dss_env][0]], format="loom")
        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self._analyze_loom_matrix_results(self.request_ids[GenusSpecies.HUMAN.value],
                                          [INPUT_BUNDLE_IDS[self.dss_env][0]])
        self.assertIn(GenusSpecies.MOUSE.value, self.request_ids)
        self.assertEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.MOUSE.value]), "")

    def test_loom_output_matrix_service(self):
        self.request_ids = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env], format="loom")
        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self._analyze_loom_matrix_results(self.request_ids[GenusSpecies.HUMAN.value],
                                          INPUT_BUNDLE_IDS[self.dss_env])
        self.assertIn(GenusSpecies.MOUSE.value, self.request_ids)
        self.assertEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.MOUSE.value]), "")

    def test_csv_output_matrix_service(self):
        self.request_ids = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env], format="csv")
        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self._analyze_csv_matrix_results(self.request_ids[GenusSpecies.HUMAN.value],
                                         INPUT_BUNDLE_IDS[self.dss_env])
        self.assertIn(GenusSpecies.MOUSE.value, self.request_ids)
        self.assertEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.MOUSE.value]), "")

    def test_mtx_output_matrix_service(self):
        self.request_ids = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env], format="mtx")
        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self._analyze_mtx_matrix_results(self.request_ids[GenusSpecies.HUMAN.value],
                                         INPUT_BUNDLE_IDS[self.dss_env])
        self.assertIn(GenusSpecies.MOUSE.value, self.request_ids)
        self.assertEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.MOUSE.value]), "")

    def test_cache_hit_matrix_service(self):
        request_ids_1 = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env], format="loom")
        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_all_requests_in_status, request_ids_1, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self._analyze_loom_matrix_results(request_ids_1[GenusSpecies.HUMAN.value],
                                          INPUT_BUNDLE_IDS[self.dss_env])

        request_ids_2 = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env], format="loom")
        # timeout seconds is reduced to 300 as cache hits do not run conversion in batch.
        WaitFor(self._poll_all_requests_in_status, request_ids_2, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self._analyze_loom_matrix_results(request_ids_2[GenusSpecies.HUMAN.value],
                                          INPUT_BUNDLE_IDS[self.dss_env])

        matrix_location_1 = self._retrieve_matrix_location(request_ids_1[GenusSpecies.HUMAN.value])
        matrix_location_2 = self._retrieve_matrix_location(request_ids_2[GenusSpecies.HUMAN.value])

        loom_metrics_1 = validation.calculate_ss2_metrics_loom(matrix_location_1)
        loom_metrics_2 = validation.calculate_ss2_metrics_loom(matrix_location_2)

        self._compare_metrics(loom_metrics_1, loom_metrics_2)

        self._cleanup_all_matrix_results(request_ids_1)
        self._cleanup_all_matrix_results(request_ids_2)

    def test_matrix_service_without_specified_output(self):
        self.request_ids = self._post_matrix_service_request(
            bundle_fqids=INPUT_BUNDLE_IDS[self.dss_env])
        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=300)
        self._analyze_loom_matrix_results(self.request_ids[GenusSpecies.HUMAN.value], INPUT_BUNDLE_IDS[self.dss_env])
        self.assertIn(GenusSpecies.MOUSE.value, self.request_ids)
        self.assertEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.MOUSE.value]), "")

    def test_matrix_service_with_unexpected_bundles(self):
        input_bundles = ['non-existent-bundle1', 'non-existent-bundle2']
        self.request_ids = self._post_matrix_service_request(
            bundle_fqids=input_bundles)
        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.FAILED.value)\
            .to_return_value(True, timeout_seconds=300)

    @unittest.skipUnless(os.getenv('DEPLOYMENT_STAGE') != "prod",
                         "Do not want to process fake notifications in production.")
    def test_dss_notification(self):
        bundle_data = NOTIFICATION_TEST_DATA[self.dss_env]
        bundle_fqid = bundle_data['bundle_fqid']
        cell_row_count = bundle_data['cell_count']
        expression_row_count = bundle_data['exp_count']

        cellkeys = format_str_list(self._get_cellkeys_from_fqid(bundle_fqid))
        self.assertTrue(len(cellkeys) > 0)

        try:
            self._post_notification(bundle_fqid=bundle_fqid, event_type="DELETE")
            WaitFor(self._poll_db_get_row_counts_for_fqid, bundle_fqid, cellkeys)\
                .to_return_value((0, 0, 0), timeout_seconds=60)

            self._post_notification(bundle_fqid=bundle_fqid, event_type="CREATE")
            WaitFor(self._poll_db_get_row_counts_for_fqid, bundle_fqid, cellkeys)\
                .to_return_value((1, cell_row_count, expression_row_count), timeout_seconds=600)

            self._post_notification(bundle_fqid=bundle_fqid, event_type="TOMBSTONE")
            WaitFor(self._poll_db_get_row_counts_for_fqid, bundle_fqid, cellkeys)\
                .to_return_value((0, 0, 0), timeout_seconds=60)

            self._post_notification(bundle_fqid=bundle_fqid, event_type="UPDATE")
            WaitFor(self._poll_db_get_row_counts_for_fqid, bundle_fqid, cellkeys)\
                .to_return_value((1, cell_row_count, expression_row_count), timeout_seconds=600)
        finally:
            self._post_notification(bundle_fqid=bundle_fqid, event_type="CREATE")

    @unittest.skip
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

        self.request_ids = self._post_matrix_service_request(
            bundle_fqids_url=bundle_fqids_url,
            format="loom")

        # wait for request to complete
        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=timeout)
        bundle_fqids = ['.'.join(el.split('\t')) for el in
                        requests.get(bundle_fqids_url).text.strip().split('\n')[1:]]

        self._analyze_loom_matrix_results(self.request_ids[GenusSpecies.HUMAN.value], bundle_fqids)

        self.assertIn(GenusSpecies.MOUSE.value, self.request_ids)
        self.assertEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.MOUSE.value]), "")

    def test_mixed_species(self):

        # Mouse bundles are not available in all environments
        if not MOUSE_BUNDLE_IDS.get(self.dss_env):
            return

        human_bundle = INPUT_BUNDLE_IDS[self.dss_env][0]
        mouse_bundle = MOUSE_BUNDLE_IDS[self.dss_env][0]
        self.request_ids = self._post_matrix_service_request(
            bundle_fqids=[human_bundle, mouse_bundle], format="loom")

        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self._analyze_loom_matrix_results(self.request_ids[GenusSpecies.HUMAN.value], [human_bundle])
        self.assertIn(GenusSpecies.MOUSE.value, self.request_ids)
        self.assertNotEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.MOUSE.value]), "")

    def test_mouse_only(self):

        # Mouse bundles are not available in all environments
        if not MOUSE_BUNDLE_IDS.get(self.dss_env):
            return
        mouse_bundle = MOUSE_BUNDLE_IDS[self.dss_env][0]
        self.request_ids = self._post_matrix_service_request(
            bundle_fqids=[mouse_bundle], format="loom")

        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self.assertEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.HUMAN.value]), "")
        self.assertNotEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.MOUSE.value]), "")

    def _poll_db_get_row_counts_for_fqid(self, bundle_fqid, cellkeys):
        """
        :return: Row counts associated with the given bundle fqid for (analysis_count, cell_count, exp_count)
        """
        analysis_query = f"SELECT COUNT(*) FROM analysis WHERE analysis.bundle_fqid = '{bundle_fqid}'"
        cell_query = f"SELECT COUNT(*) FROM cell WHERE cellkey IN {cellkeys}"
        exp_query = f"SELECT COUNT(*) FROM expression WHERE cellkey IN {cellkeys}"

        analysis_row_count = self.redshift_handler.transaction([analysis_query], return_results=True)[0][0]
        cell_row_count = self.redshift_handler.transaction([cell_query], return_results=True)[0][0]
        exp_row_count = self.redshift_handler.transaction([exp_query], return_results=True)[0][0]

        return analysis_row_count, cell_row_count, exp_row_count

    def _get_cellkeys_from_fqid(self, bundle_fqid):
        """
        Returns a generator of cellkeys associated with a given bundle fqid.
        """
        cellkeys_query = f"""
        SELECT DISTINCT cellkey FROM cell
         JOIN analysis ON cell.analysiskey = analysis.analysiskey
         WHERE analysis.bundle_fqid = '{bundle_fqid}'
        """

        results = self.redshift_handler.transaction([cellkeys_query], return_results=True)
        return (row[0] for row in results)

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
        request_ids = {GenusSpecies.HUMAN.value: data["request_id"]}
        request_ids.update(data["non_human_request_ids"])
        return request_ids

    def _post_notification(self, bundle_fqid, event_type):
        data = {}
        bundle_uuid = bundle_fqid.split('.', 1)[0]
        bundle_version = bundle_fqid.split('.', 1)[1]
        url = f"{self.api_url[:-3]}/dss/notification"
        config = MatrixInfraConfig()

        data["transaction_id"] = "test_transaction_id"
        data["subscription_id"] = "test_subscription_id"
        data["event_type"] = event_type
        data["match"] = {}
        data["match"]["bundle_uuid"] = bundle_uuid
        data["match"]["bundle_version"] = bundle_version

        response = requests.post(url=url,
                                 json=data,
                                 auth=HTTPSignatureAuth(key_id=DSS_SUBSCRIPTION_HMAC_SECRET_ID,
                                                        key=config.dss_subscription_hmac_secret_key.encode()))

        print(f"POST NOTIFICATION TO MATRIX SERVICE: \nPOST {url}\n-> {response.status_code}")
        if response.content:
            print(response.content.decode('utf8'))


class TestMatrixServiceV1(MatrixServiceTest):
    def setUp(self):
        self.dss_env = MATRIX_ENV_TO_DSS_ENV[os.environ['DEPLOYMENT_STAGE']]
        self.api_url = f"https://{os.environ['API_HOST']}/v1"
        self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        self.verbose = True
        self.s3_file_system = s3fs.S3FileSystem(anon=False)

    def tearDown(self):
        if hasattr(self, 'request_ids') and self.request_ids:
            self._cleanup_all_matrix_results(self.request_ids)

    def _post_matrix_service_request(self, filter_, fields=None, feature=None, format_=None):
        data = {"filter": filter_}
        if fields:
            data["fields"] = fields
        if feature:
            data["feature"] = feature
        if format_:
            data["format"] = format_

        response = self._make_request(description="POST REQUEST TO MATRIX SERVICE",
                                      verb='POST',
                                      url=f"{self.api_url}/matrix",
                                      expected_status=202,
                                      data=json.dumps(data),
                                      headers=self.headers)

        data = json.loads(response)
        request_ids = {GenusSpecies.HUMAN.value: data["request_id"]}
        request_ids.update(data["non_human_request_ids"])
        return request_ids

    def test_mtx_output_matrix_service(self):

        request_ids_1 = self._post_matrix_service_request(
            filter_={"op": "in",
                     "field": "dss_bundle_fqid",
                     "value": INPUT_BUNDLE_IDS[self.dss_env]},
            format_="mtx")

        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_all_requests_in_status, request_ids_1, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self._analyze_mtx_matrix_results(request_ids_1[GenusSpecies.HUMAN.value], INPUT_BUNDLE_IDS[self.dss_env])
        self.assertEqual(len(request_ids_1), 1)

        with self.subTest("Testing cache hit path with explicit parameters"):
            request_ids_2 = self._post_matrix_service_request(
                filter_={"op": "in",
                         "field": "dss_bundle_fqid",
                         "value": INPUT_BUNDLE_IDS[self.dss_env]},
                format_="mtx",
                fields=DEFAULT_FIELDS,
                feature=DEFAULT_FEATURE)
            # timeout seconds is reduced to 300 as cache hits do not run conversion in batch.
            WaitFor(self._poll_all_requests_in_status, request_ids_2, MatrixRequestStatus.COMPLETE.value)\
                .to_return_value(True, timeout_seconds=300)
            self._analyze_mtx_matrix_results(request_ids_2[GenusSpecies.HUMAN.value], INPUT_BUNDLE_IDS[self.dss_env])

            matrix_location_1 = self._retrieve_matrix_location(request_ids_1[GenusSpecies.HUMAN.value])
            matrix_location_2 = self._retrieve_matrix_location(request_ids_2[GenusSpecies.HUMAN.value])

            mtx_metrics_1 = validation.calculate_ss2_metrics_mtx(matrix_location_1)
            mtx_metrics_2 = validation.calculate_ss2_metrics_mtx(matrix_location_2)

            self._compare_metrics(mtx_metrics_1, mtx_metrics_2)

            self._cleanup_all_matrix_results(request_ids_1)
            self._cleanup_all_matrix_results(request_ids_2)

    def test_request_fields(self):

        fields = ["derived_organ_label", "dss_bundle_fqid", "genes_detected",
                  "library_preparation_protocol.library_construction_method.ontology"]
        self.request_ids = self._post_matrix_service_request(
            filter_={"op": "in",
                     "field": "dss_bundle_fqid",
                     "value": INPUT_BUNDLE_IDS[self.dss_env]},
            format_="csv",
            fields=fields)

        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)

        matrix_location = self._retrieve_matrix_location(self.request_ids[GenusSpecies.HUMAN.value])

        temp_dir = tempfile.mkdtemp(suffix="csv_fields_test")
        local_csv_zip_path = os.path.join(temp_dir, os.path.basename(matrix_location))
        response = requests.get(matrix_location, stream=True)
        with open(local_csv_zip_path, "wb") as local_csv_zip_file:
            shutil.copyfileobj(response.raw, local_csv_zip_file)
        csv_zip = zipfile.ZipFile(local_csv_zip_path)
        cells_name = [n for n in csv_zip.namelist() if n.endswith("cells.csv")][0]

        cells_pdata = pandas.read_csv(
            io.StringIO(csv_zip.read(cells_name).decode()),
            header=0,
            index_col=0)

        self.assertListEqual(list(cells_pdata.columns), fields)

    @unittest.skipUnless(os.getenv('DEPLOYMENT_STAGE') in ("dev", "prod"),
                         "Only test filters against known bundles in prod")
    def test_ops(self):

        # Filter should return four of the five test bundles
        self.request_ids = self._post_matrix_service_request(
            filter_={"op": "and",
                     "value": [
                         {"op": "=",
                          "field": "library_preparation_protocol.library_construction_method.ontology",
                          "value": "EFO:0008931"},
                         {"op": "!=",
                          "field": "derived_organ_label",
                          "value": "decidua"},
                         {"op": "in",
                          "field": "dss_bundle_fqid",
                          "value": INPUT_BUNDLE_IDS[self.dss_env]}]},
            format_="loom")

        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        matrix_location = self._retrieve_matrix_location(self.request_ids[GenusSpecies.HUMAN.value])

        temp_dir = tempfile.mkdtemp(suffix="loom_ops_test")
        local_loom_path = os.path.join(temp_dir, os.path.basename(matrix_location))
        response = requests.get(matrix_location, stream=True)
        with open(local_loom_path, "wb") as local_loom_file:
            shutil.copyfileobj(response.raw, local_loom_file)

        ds = loompy.connect(local_loom_path)

        self.assertEqual(ds.shape[1], 4)

    def test_mouse_only(self):

        # Mouse bundles are not yet available in every environment
        if not MOUSE_BUNDLE_IDS[self.dss_env]:
            return
        mouse_bundle_uuid = MOUSE_BUNDLE_IDS[self.dss_env][0].split('.', 1)[0]

        filter_ = {
            "op": "and",
            "value": [
                {
                    "op": "=",
                    "field": "cell_suspension.genus_species.ontology_label",
                    "value": GenusSpecies.MOUSE.value
                },
                {
                    "op": "=",
                    "field": "bundle_uuid",
                    "value": mouse_bundle_uuid
                }
            ]
        }
        self.request_ids = self._post_matrix_service_request(filter_, format_="loom")

        WaitFor(self._poll_all_requests_in_status, self.request_ids, MatrixRequestStatus.COMPLETE.value)\
            .to_return_value(True, timeout_seconds=1200)
        self.assertEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.HUMAN.value]), "")
        self.assertNotEqual(self._retrieve_matrix_location(self.request_ids[GenusSpecies.MOUSE.value]), "")

    def test_filter_detail(self):

        response = self._make_request(description="GET REQUEST TO FILTER DETAIL",
                                      verb='GET',
                                      url=f"{self.api_url}/filters/dss_bundle_fqid",
                                      expected_status=200,
                                      headers=self.headers)
        cell_counts = json.loads(response.decode())["cell_counts"]

        # The test bundles should show up in the response, and since they're
        # smart-seq2, they should have a cell count of 1
        for bundle_fqid in INPUT_BUNDLE_IDS[self.dss_env]:
            self.assertIn(bundle_fqid, cell_counts)
            self.assertEqual(cell_counts[bundle_fqid], 1)

    def test_filter_detail_in_cell_table(self):

        response = self._make_request(description="GET REQUEST TO FILTER DETAIL",
                                      verb='GET',
                                      url=f"{self.api_url}/filters/genes_detected",
                                      expected_status=200,
                                      headers=self.headers)
        response = json.loads(response.decode())
        self.assertIn("minimum", response)
        self.assertIn("maximum", response)

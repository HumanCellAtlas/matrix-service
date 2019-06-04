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

from . import validation
from .wait_for import WaitFor
from matrix.common.constants import MATRIX_ENV_TO_DSS_ENV, MatrixRequestStatus
from matrix.common.aws.redshift_handler import RedshiftHandler


INPUT_BUNDLE_IDS = {
    "integration": [
        "5cb665f4-97bb-4176-8ec2-1b83b95c1bc0.2019-02-11T171739.925160Z",
        "ff7ef351-f46f-4c39-b4c3-c8b33423a4c9.2019-02-11T124842.494942Z",
        "aa8262c2-7a0e-49fd-bac1-d41a4019bd87.2019-02-10T234926.510991Z",
        "0ef88e4a-a779-4588-8677-953d65ca6d9a.2019-02-10T124405.139571Z",
        "c881020e-9f53-4f7e-9c49-d9dbd9e8f280.2019-02-09T124912.755814Z",
    ],
    "staging": [
        "119f6f39-d111-4c33-a3d5-224a67655b07.2018-10-24T224220.927365Z",
        "2043408c-6247-45ee-bbc1-4b55fc0a7e43.2018-10-24T222330.993761Z",
        "e2c1cd87-b051-49b3-becd-b52a41b6a9e4.2018-10-24T222835.237092Z",
        "9c8c5ba1-27fb-496f-9dfe-b605a9aa9658.2018-10-24T232635.721854Z",
        "7ba67071-6fb3-43f8-ada8-ed5993195e2b.2018-10-24T225455.712647Z",
    ],
    "prod": [
        "4d825362-dc29-4135-9f53-92c27955ddda.2019-02-02T072347.000888Z",
        "ae066cbd-f9f8-438a-8c3c-43f5cd44a9eb.2019-02-02T042049.843007Z",
        "684a7318-a405-450b-a616-f97ddbc34f8f.2019-02-01T183026.372619Z",
        "e83d8b22-2090-4f34-868d-92a8749a401d.2019-02-02T021732.127000Z",
        "2c1160e9-2324-4dfa-9776-d93f76d31fde.2018-11-21T140133.660897Z",
    ]
}

NOTIFICATION_BUNDLE_IDS = {
    "integration": "5cb665f4-97bb-4176-8ec2-1b83b95c1bc0.2019-02-11T171739.925160Z",
    "staging": "119f6f39-d111-4c33-a3d5-224a67655b07.2018-10-24T224220.927365Z",
    # notification test does not run on prod, however other matrix environments may point to dss prod
    "prod": "fffe55c1-18ed-401b-aa9a-6f64d0b93fec.2019-05-17T233932.932000Z",
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

    def test_single_bundle_request(self):
        self.request_id = self._post_matrix_service_request(
            bundle_fqids=[INPUT_BUNDLE_IDS[self.dss_env][0]], format="loom")
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=1200)
        self._analyze_loom_matrix_results(self.request_id, [INPUT_BUNDLE_IDS[self.dss_env][0]])

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
        bundle_fqid = NOTIFICATION_BUNDLE_IDS[self.dss_env]
        try:
            self._post_notification(bundle_fqid=bundle_fqid, event_type="DELETE")
            WaitFor(self._poll_db_get_row_counts_from_fqid, bundle_fqid)\
                .to_return_value((0, 0, 0), timeout_seconds=60)

            self._post_notification(bundle_fqid=bundle_fqid, event_type="CREATE")
            WaitFor(self._poll_db_get_row_counts_from_fqid, bundle_fqid)\
                .to_return_value_in_range((1, 1, 20000), (1, 1, 25000), timeout_seconds=600)

            self._post_notification(bundle_fqid=bundle_fqid, event_type="TOMBSTONE")
            WaitFor(self._poll_db_get_row_counts_from_fqid, bundle_fqid)\
                .to_return_value((0, 0, 0), timeout_seconds=60)

            self._post_notification(bundle_fqid=bundle_fqid, event_type="UPDATE")
            WaitFor(self._poll_db_get_row_counts_from_fqid, bundle_fqid)\
                .to_return_value_in_range((1, 1, 20000), (1, 1, 25000), timeout_seconds=600)
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

        self.request_id = self._post_matrix_service_request(
            bundle_fqids_url=bundle_fqids_url,
            format="loom")

        # wait for request to complete
        WaitFor(self._poll_get_matrix_service_request, self.request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=timeout)
        bundle_fqids = ['.'.join(el.split('\t')) for el in
                        requests.get(bundle_fqids_url).text.strip().split('\n')[1:]]

        self._analyze_loom_matrix_results(self.request_id, bundle_fqids)

    def _poll_db_get_row_counts_from_fqid(self, bundle_fqid):
        """
        :param bundle_fqid: Bundle fqid to query Redshift for
        :return: Row counts for (analysis_count, cell_count, exp_count)
        """
        analysis_query = f"SELECT count(*) from analysis where analysis.bundle_fqid = '{bundle_fqid}'"
        cell_query = f"SELECT count(cell.cellkey) from cell " \
                     f"join analysis on cell.analysiskey = analysis.analysiskey " \
                     f"where analysis.bundle_fqid = '{bundle_fqid}'"
        exp_query = f"SELECT count(*) from cell " \
                    f"join analysis on cell.analysiskey = analysis.analysiskey " \
                    f"join expression on cell.cellkey = expression.cellkey " \
                    f"where analysis.bundle_fqid = '{bundle_fqid}'"

        analysis_row_count = self.redshift_handler.transaction([analysis_query], return_results=True)[0][0]
        cell_row_count = self.redshift_handler.transaction([cell_query], return_results=True)[0][0]
        exp_row_count = self.redshift_handler.transaction([exp_query], return_results=True)[0][0]

        return analysis_row_count, cell_row_count, exp_row_count

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


class TestMatrixServiceV1(MatrixServiceTest):
    def setUp(self):
        self.dss_env = MATRIX_ENV_TO_DSS_ENV[os.environ['DEPLOYMENT_STAGE']]
        self.api_url = f"https://{os.environ['API_HOST']}/v1"
        self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        self.verbose = True
        self.s3_file_system = s3fs.S3FileSystem(anon=False)

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
        return data["request_id"]

    def test_mtx_output_matrix_service(self):

        self.request_id = self._post_matrix_service_request(
            filter_={"op": "in",
                     "field": "dss_bundle_fqid",
                     "value": INPUT_BUNDLE_IDS[self.dss_env]},
            format_="mtx")

        # timeout seconds is increased to 1200 as batch may take time to spin up spot instances for conversion.
        WaitFor(self._poll_get_matrix_service_request, self.request_id) \
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=1200)
        self._analyze_mtx_matrix_results(self.request_id, INPUT_BUNDLE_IDS[self.dss_env])

    def test_request_fields(self):

        fields = ["derived_organ_label", "dss_bundle_fqid", "genes_detected",
                  "library_preparation_protocol.library_construction_method.ontology"]
        self.request_id = self._post_matrix_service_request(
            filter_={"op": "in",
                     "field": "dss_bundle_fqid",
                     "value": INPUT_BUNDLE_IDS[self.dss_env]},
            format_="csv",
            fields=fields)

        WaitFor(self._poll_get_matrix_service_request, self.request_id) \
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=1200)

        matrix_location = self._retrieve_matrix_location(self.request_id)

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

        # Filter should return two of the five test bundles
        self.request_id = self._post_matrix_service_request(
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

        WaitFor(self._poll_get_matrix_service_request, self.request_id) \
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=1200)
        matrix_location = self._retrieve_matrix_location(self.request_id)

        temp_dir = tempfile.mkdtemp(suffix="loom_ops_test")
        local_loom_path = os.path.join(temp_dir, os.path.basename(matrix_location))
        response = requests.get(matrix_location, stream=True)
        with open(local_loom_path, "wb") as local_loom_file:
            shutil.copyfileobj(response.raw, local_loom_file)

        ds = loompy.connect(local_loom_path)

        self.assertEqual(ds.shape[1], 2)

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

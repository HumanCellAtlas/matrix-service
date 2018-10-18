import unittest
import os
import requests
import json

import s3fs
import zarr
import numpy

from .wait_for import WaitFor
from matrix.common.constants import MatrixRequestStatus
from matrix.common.pandas_utils import convert_dss_zarr_root_to_subset_pandas_dfs


INPUT_BUNDLE_IDS = {
    "dev": [
        "ca8308ee-8388-44d6-b4c3-dea6da1334f1.2018-10-18T001232.039329Z",
        "8a82f068-d324-428c-82ad-14def15442c3.2018-10-17T235815.275412Z",
        "3b196d5e-7d88-4d02-9c6f-61271764e4ba.2018-10-17T235315.092228Z",
        "bcd855b6-e39a-4f55-a2be-9c0d094510e2.2018-10-17T234649.368503Z",
        "b51f5de3-4d65-484a-bd16-7e0bb1a6df59.2018-10-17T234148.910728Z",
    ],
    "integration": [
        '0f997914-43c2-45e2-b79f-99167295b263.2018-10-17T204940.626010Z',
        '167a2b69-f52f-4a0a-9691-d1db62ef12de.2018-10-17T201019.320177Z',
        'b2965ca9-4aca-4baf-9606-215508d1e475.2018-10-17T200207.329078Z',
        '8d567bed-a9aa-4a39-9467-75510b965257.2018-10-17T191234.528671Z',
        'ba9c63ac-6db5-48bc-a2e3-7be4ddd03d97.2018-10-17T173508.111787Z',
    ],
    "staging": []
}


class TestMatrixService(unittest.TestCase):

    def setUp(self):
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']
        self.api_url = f"https://{os.environ['API_HOST']}/v0"
        self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        self.verbose = True
        self.s3_file_system = s3fs.S3FileSystem(anon=False)

    def test_matrix_service(self):
        # make request and receive job id back
        request_id = self._post_matrix_service_request()

        # wait for get requests to return 200 and status of COMPLETED
        WaitFor(self._poll_get_matrix_service_request, request_id)\
            .to_return_value(MatrixRequestStatus.COMPLETE.value, timeout_seconds=300)

        # analyze produced matrix against standard
        self._analyze_matrix_results(request_id)

    def _post_matrix_service_request(self):
        data = {
            "bundle_fqids": INPUT_BUNDLE_IDS[self.deployment_stage],
            "format": "zarr"
        }
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

    def _analyze_matrix_results(self, request_id):
        matrix_location = self._retrieve_matrix_location(request_id)
        store = s3fs.S3Map(matrix_location, s3=self.s3_file_system, check=False, create=False)
        group = zarr.group(store=store)
        exp_df, qc_df = convert_dss_zarr_root_to_subset_pandas_dfs(group, 0, 5)
        exp_df_sum = numpy.sum(exp_df.values)
        self.assertEqual(exp_df_sum, 4999999.0)
        self.assertEqual(qc_df.shape, (5, 154))
        self.assertEqual(exp_df.shape, (5, 58347))

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

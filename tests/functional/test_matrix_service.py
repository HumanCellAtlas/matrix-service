import unittest
import os
import requests
import json

from .waitfor import WaitFor


INPUT_BUNDLE_IDS = {
    "dev": [],
    "integration": [
        "8e60fe1f-2217-4a15-9c2f-4e79cc341196",
        "72174501-a888-4a86-8555-8c6e8d16b3f0",
        "1326e6ed-2a1c-4009-95b5-8458c82e24f4",
        "f0a2cbce-b932-4700-9032-2e99516e6caa",
        "fd6b9d57-1f5a-481d-bd75-f2702dc5b610",
        "b5edd2e7-d588-4dda-b1cb-409d9d32a97e",
    ],
    "staging": []
}


class TestMatrixService(unittest.TestCase):

    def setUp(self):
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']
        self.api_url = f"https://{os.environ['API_HOST']}/v0"
        self.verbose = True

    def test_matrix_service(self):
        # make request and receive job id back
        self._create_matrix_service_request()

        # wait for get requests to return 200 and status of COMPLETED
        WaitFor(self._poll_matrix_service_request)\
            .to_return_value('COMPLETED', timeout_seconds=300)

        # analyze produced matrix against standard
        self._analyze_matrix_results()

    def _create_matrix_service_request(self):
        response = self._make_request(description="POST REQUEST TO MATRIX SERVICE",
                                      verb='POST',
                                      url=f"{self.api_url}/matrix",
                                      expected_status=202,
                                      json={"bundle_fqids": INPUT_BUNDLE_IDS[self.deployment_stage]})
        data = json.loads(response)
        self.request_id = data["request_id"]

    def _poll_matrix_service_request(self):
        response = self._make_request(description="GET REQUEST TO MATRIX SERVICE WITH REQUEST ID",
                                      verb='GET',
                                      url=f"{self.api_url}/matrix?request_id={self.request_id}",
                                      expected_status=200)
        data = json.loads(response)
        status = data["status"]
        if status == "Completed":
            self.results_s3_key = data["s3_results_key"]
        return status

    def _analyze_matrix_results(self):
        # THIS NEEDS TO BE IMPLEMENTED ONCE WE HAVE A FUNCTIONAL MATRIX SERVICE THAT RETURNS S3 LOCATION OF RESULTS
        # 1) PULL DOWN FILE FROM S3
        # 2) OPEN FILE CONTENT WITH PANDAS
        # 3) ANALYZE RESULTS
        #    a) the resulting `expression` data should have a sum of ~6000000 and a nonzero element count of 606
        #    b) the cell_metadata should have a sum of 3515992 and a nonzero element count of 762
        pass

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

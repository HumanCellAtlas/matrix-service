import boto3
import json
import os
import random
import requests
import sys
import time

from locust import HttpLocust, TaskSet, task, events, stats

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from matrix.common import date
from tests.functional.wait_for import WaitFor

stats.CSV_STATS_INTERVAL_SEC = 5
OUTPUT_FORMATS = ["zarr", "loom", "mtx", "csv"]


class UserBehavior(TaskSet):
    """
    Defines the set of tasks that a simulated user will execute against the system
    """
    INPUT_SIZES = [1]
    INPUT_SIZES.extend([(i + 1) * 300 for i in range(6)])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.matrix_client = MatrixClient()

    def on_start(self):
        """
        Runs before a task is scheduled
        """
        print("starting")

    def on_stop(self):
        """
        Runs after a task is completed
        """
        print("stopping")

    @task
    def test_format_with_multiple_sizes(self):
        """
        Synchronously issues multiple POST requests to /matrix with various
        bundle input sizes for a randomly selected output format.
        """
        matrix_format = random.choice(OUTPUT_FORMATS)
        for size in UserBehavior.INPUT_SIZES:
            self.matrix_client.request_matrix("SS2", size, matrix_format)


class LocustTest(HttpLocust):
    """
    Defines the parameters of the test such as user tasks, test set up and tear down
    """
    task_set = UserBehavior
    min_wait = 3000
    max_wait = 5000

    def setup(self):
        self.timestamp = date.get_datetime_now(as_string=True)
        print(f"Running test with timestamp {self.timestamp}")

    def teardown(self):
        self.export_results_to_s3()

    def export_results_to_s3(self):
        """
        Exports and uploads Grafana dashboards to S3
        """
        s3 = boto3.resource("s3", region_name=os.environ['AWS_DEFAULT_REGION'])

        snapshot_obj = self._export_grafana_snapshot()
        version = self._get_matrix_version()

        results = {
            'grafana': {
                'snapshot_url': snapshot_obj['url'],
                'delete_snapshot_url': snapshot_obj['deleteUrl']
            },
            'matrix_version': version,
            'timestamp': self.timestamp
        }

        s3.Bucket("dcp-matrix-service-performance-results"). \
            put_object(Key=f"{os.environ['DEPLOYMENT_STAGE']}-{self.timestamp}.txt",
                       Body=json.dumps(results).encode('utf-8'))

    def _get_matrix_version(self):
        response = requests.get(f"https://matrix.{os.environ['DEPLOYMENT_STAGE']}.data.humancellatlas.org/version")
        return json.loads(response.content)['version_info']['version']

    def _export_grafana_snapshot(self):
        secrets = boto3.client('secretsmanager', region_name=os.environ['AWS_DEFAULT_REGION'])
        auth = secrets.get_secret_value(SecretId="grafana/admin_credentials")['SecretString']

        dashboard_url = f"https://{auth}@metrics.dev.data.humancellatlas.org/" \
                        f"api/dashboards/uid/matrix-{os.environ['DEPLOYMENT_STAGE']}"
        snapshot_url = f"https://{auth}@metrics.dev.data.humancellatlas.org/api/snapshots"

        response = requests.get(dashboard_url)
        dashboard_obj = json.loads(response.content)['dashboard']

        response = requests.post(snapshot_url,
                                 data=json.dumps({'dashboard': dashboard_obj}),
                                 headers={'Content-Type': "application/json"})

        return json.loads(response.content)


class MatrixClient:
    """
    Client for interacting with the Matrix Service
    """

    # Test bundle sets organized by assay type and deployment stage
    BUNDLE_FQIDS = {
        'SS2': {
            'dev': [
                "5cb665f4-97bb-4176-8ec2-1b83b95c1bc0.2019-02-11T171739.925160Z",
                "ff7ef351-f46f-4c39-b4c3-c8b33423a4c9.2019-02-11T124842.494942Z",
                "aa8262c2-7a0e-49fd-bac1-d41a4019bd87.2019-02-10T234926.510991Z",
                "0ef88e4a-a779-4588-8677-953d65ca6d9a.2019-02-10T124405.139571Z",
                "c881020e-9f53-4f7e-9c49-d9dbd9e8f280.2019-02-09T124912.755814Z",
            ],
            'integration': [
                "5cb665f4-97bb-4176-8ec2-1b83b95c1bc0.2019-02-11T171739.925160Z",
                "ff7ef351-f46f-4c39-b4c3-c8b33423a4c9.2019-02-11T124842.494942Z",
                "aa8262c2-7a0e-49fd-bac1-d41a4019bd87.2019-02-10T234926.510991Z",
                "0ef88e4a-a779-4588-8677-953d65ca6d9a.2019-02-10T124405.139571Z",
                "c881020e-9f53-4f7e-9c49-d9dbd9e8f280.2019-02-09T124912.755814Z",
            ],
            'staging': json.loads(open("../functional/res/pancreas_ss2_2544_demo_bundles.json", "r").read()),
            'prod': [
                "0552e6b3-ee09-425e-adbb-01fb9467e6f3.2018-11-06T231250.330922Z",
                "79e14c18-7bf7-4883-92a1-b7b26c3067d4.2018-11-06T232117.884033Z",
                "1134eb98-e7e9-45af-b2d8-b9886b633929.2018-11-06T231738.177145Z",
                "1d6de514-115f-4ed6-8d11-22ad02e394bc.2018-11-06T231755.376078Z",
                "2ec96e8c-6d28-4d98-9a19-7c95bbe13ce2.2018-11-06T231809.821560Z",
            ],
        },
        '10X': {
            'dev': [],
            'integration': [],
            'staging': [],
            'prod': [],
        }
    }

    def __init__(self, *args, **kwargs):
        self.stage = os.environ['DEPLOYMENT_STAGE']
        self.host = f"https://matrix.{'' if self.stage == 'prod' else self.stage + '.'}data.humancellatlas.org/v0"

    def request_matrix(self, assay_type, input_size, output_format):
        """
        Request and wait for a matrix with a specified assay type, number of bundles and output format
        :param assay_type: Type of assay used to produce set of input bundles
        :param input_size: Requested number of bundles in request
        :param output_format: Requested output format
        """
        request_name = f"{assay_type}_{input_size}_{output_format}"
        request_type = output_format
        bundle_fqids = random.sample(MatrixClient.BUNDLE_FQIDS[assay_type][self.stage], input_size)
        start_time = time.time()
        post_response = requests.post(f"{self.host}/matrix", json={'bundle_fqids': bundle_fqids,
                                                                   'format': output_format})
        request_id = json.loads(post_response.content)['request_id']

        try:
            WaitFor(self._get_matrix, request_id, backoff_factor=1.1).to_return_value("Complete", timeout_seconds=300)
            events.request_success.fire(request_type=request_type,
                                        name=request_name,
                                        response_time=time.time() - start_time,
                                        response_length=input_size)
        except RuntimeError as e:
            events.request_failure.fire(request_type=request_type,
                                        name=request_name,
                                        response_time=time.time() - start_time,
                                        exception=f"Matrix request failed: {str(e)}")

    def _get_matrix(self, request_id):
        """
        GET /matrix
        :param request_id: Request ID supplied by POST /matrix
        :return: Status of request (In Progress | Complete | Failed)
        """
        url = f"{self.host}/matrix/{request_id}"
        response = requests.get(url, headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        data = json.loads(response.content)
        return data["status"]

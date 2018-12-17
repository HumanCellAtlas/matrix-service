import json
import os
import random
import requests
import sys
import time
import uuid

import matplotlib.pyplot as plt
import pandas as pd
from locust import HttpLocust, TaskSet, task, events, stats

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.functional.wait_for import WaitFor

stats.CSV_STATS_INTERVAL_SEC = 5
OUTPUT_FORMATS = ["zarr", "loom", "mtx", "csv"]


class UserBehavior(TaskSet):
    """
    Defines the set of tasks that a simulated user will execute against the system
    """
    INPUT_SIZES = [1]
    INPUT_SIZES.extend([(i + 1) * 200 for i in range(5)])

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
        self.uuid = str(uuid.uuid4())
        print(f"Running test with test ID {self.uuid}")
        os.mkdir(self.uuid)

    def teardown(self):
        os.rename("results_requests.csv", f"{self.uuid}/results_requests.csv")
        os.rename("results_distribution.csv", f"{self.uuid}/results_distribution.csv")
        self.plot_results()

    def export_grafana_panels_to_s3(self):
        """
        Exports and uploads Processing Times panels from Grafana to S3 buckets
        """
        return

    def plot_results(self):
        """
        Generates plots of the processing time results for each and all output formats
        """
        print(f"Plotting results for test ID {self.uuid}")
        df = pd.read_csv(f"{self.uuid}/results_requests.csv")
        df = df.drop(len(df) - 1)
        format_to_color = {
            'zarr': 'm',
            'loom': 'c',
            'mtx': 'r',
            'csv': 'y',
        }
        format_to_size = {
            'zarr': 32,
            'loom': 22,
            'mtx': 12,
            'csv': 2,
        }

        results = {}

        # generate graphs per format
        for matrix_format in OUTPUT_FORMATS:
            format_results = results[matrix_format] = df[df['Method'] == matrix_format]

            plt.errorbar(x=format_results['Average Content Size'],
                         y=format_results['Average response time'],
                         label=matrix_format,
                         fmt=f"{format_to_color[matrix_format]}_",
                         yerr=(format_results['Max response time'] - format_results['Average response time'],
                               format_results['Average response time'] - format_results['Min response time']))
            plt.xlabel("# of bundles")
            plt.ylabel("Response time (s)")
            plt.legend(loc="lower right")
            plt.title(f"{matrix_format.capitalize()} Request Profile")
            plt.savefig(f"{self.uuid}/{matrix_format}_plot.pdf")
            plt.close()

        # generate graph with all formats
        for matrix_format in OUTPUT_FORMATS:
            format_results = results[matrix_format]
            plt.errorbar(x=format_results['Average Content Size'],
                         y=format_results['Average response time'],
                         label=matrix_format,
                         fmt=f"{format_to_color[matrix_format]}_",
                         yerr=(format_results['Max response time'] - format_results['Average response time'],
                               format_results['Average response time'] - format_results['Min response time']),
                         elinewidth=format_to_size[matrix_format],
                         markersize=format_to_size[matrix_format] + 8)

        plt.xlabel("# of bundles")
        plt.ylabel("Processing time (s)")
        plt.legend(loc="lower right")
        plt.title("Matrix Service Request Profile")
        plt.savefig(f"{self.uuid}/combined_plot.pdf")
        print(f"Plots and results generated in {self.uuid}/")


class MatrixClient:
    """
    Client for interacting with the Matrix Service
    """

    # Test bundle sets organized by assay type and deployment stage
    BUNDLE_FQIDS = {
        'SS2': {
            'dev': [
                "0f997914-43c2-45e2-b79f-99167295b263.2018-10-17T204940.626010Z",
                "167a2b69-f52f-4a0a-9691-d1db62ef12de.2018-10-17T201019.320177Z",
                "b2965ca9-4aca-4baf-9606-215508d1e475.2018-10-17T200207.329078Z",
                "8d567bed-a9aa-4a39-9467-75510b965257.2018-10-17T191234.528671Z",
                "ba9c63ac-6db5-48bc-a2e3-7be4ddd03d97.2018-10-17T173508.111787Z",
            ],
            'integration': [
                "0f997914-43c2-45e2-b79f-99167295b263.2018-10-17T204940.626010Z",
                "167a2b69-f52f-4a0a-9691-d1db62ef12de.2018-10-17T201019.320177Z",
                "b2965ca9-4aca-4baf-9606-215508d1e475.2018-10-17T200207.329078Z",
                "8d567bed-a9aa-4a39-9467-75510b965257.2018-10-17T191234.528671Z",
                "ba9c63ac-6db5-48bc-a2e3-7be4ddd03d97.2018-10-17T173508.111787Z",
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

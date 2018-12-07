import unittest
import os

from botocore.stub import Stubber

from matrix.common.aws.cloudwatch_handler import CloudwatchHandler, MetricName


class TestClouddwatchHandler(unittest.TestCase):
    """
    Environment variables are set in tests/unit/__init__.py
    """
    def setUp(self):
        self.deploment_stage = os.environ["DEPLOYMENT_STAGE"]
        self.handler = CloudwatchHandler()
        self.mock_cloudwatch_client = Stubber(self.handler._client)

    def test_put_metric_data(self):
        metric_data = {'MetricName': MetricName.REQUEST.value, 'Value': 1}
        expected_params = {'MetricData': [metric_data],
                           'Namespace': f"dcp-matrix-service-{self.deploment_stage}"}
        self.mock_cloudwatch_client.add_response('put_metric_data', {}, expected_params)
        self.mock_cloudwatch_client.activate()
        self.handler.put_metric_data(MetricName.REQUEST, 1)

import os
from enum import Enum

import boto3


class MetricName(Enum):
    """
    MetricNames for cloudwatch custom metric datapoints.
    """
    REQUEST = "Matrix Request"
    REQUEST_COMPLETION = "Matrix Request Completion"
    REQUEST_ERROR = "Matrix Request Error"
    CONVERSION_REQUEST = "Matrix Conversion Request"
    CONVERSION_COMPLETION = "Matrix Conversion Completion"
    CACHE_HIT = "Matrix Cache Hit"
    CACHE_MISS = "Matrix Cache Miss"


class CloudwatchHandler:
    def __init__(self):
        self.namespace = f"dcp-matrix-service-{os.environ['DEPLOYMENT_STAGE']}"
        self._client = boto3.client("cloudwatch", region_name=os.environ['AWS_DEFAULT_REGION'])

    def put_metric_data(self, metric_name: MetricName, metric_value: int):
        """
        Puts a cloudwatch metric data point

        :param metric_name: The MetricName of the metric to put
        :param value: value of metric to put
        """
        self._client.put_metric_data(
            MetricData=[
                {
                    'MetricName': metric_name.value,
                    'Value': metric_value
                },
            ],
            Namespace=self.namespace
        )

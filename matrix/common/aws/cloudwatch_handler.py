import os
from enum import Enum
import typing

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
    DURATION = "Matrix Request Duration"


class CloudwatchHandler:
    def __init__(self):
        self.namespace = f"dcp-matrix-service-{os.environ['DEPLOYMENT_STAGE']}"
        self._client = boto3.client("cloudwatch", region_name=os.environ['AWS_DEFAULT_REGION'])

    def put_metric_data(self,
                        metric_name: MetricName,
                        metric_value: typing.Union[int, float],
                        metric_dimensions: typing.List[dict]=()):
        """
        Puts a cloudwatch metric data point

        :param metric_name: The MetricName of the metric to put
        :param metric_value: value of metric to put
        :param metric_dimensions: Optional dimensions describing the metric
        """
        self._client.put_metric_data(
            MetricData=[
                {
                    'MetricName': metric_name.value,
                    'Value': metric_value,
                    'Dimensions': metric_dimensions,
                },
            ],
            Namespace=self.namespace
        )

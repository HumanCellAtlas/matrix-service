import uuid
import os
import unittest
from unittest import mock

from botocore.stub import Stubber

from matrix.common.aws.batch_handler import BatchHandler
from matrix.common.aws.cloudwatch_handler import MetricName


class TestBatchHandler(unittest.TestCase):

    def setUp(self):
        self.request_id = str(uuid.uuid4())

        self.batch_handler = BatchHandler()
        self.mock_batch_client = Stubber(self.batch_handler._client)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.batch_handler.BatchHandler._enqueue_batch_job")
    def test_schedule_matrix_conversion(self, mock_enqueue_batch_job, mock_cw_put):
        format = "test_format"
        job_name = f"conversion-{os.environ['DEPLOYMENT_STAGE']}-{self.request_id}-{format}"

        self.batch_handler.schedule_matrix_conversion(self.request_id, format, "test_s3_key")
        mock_enqueue_batch_job.assert_called_once_with(job_name=job_name,
                                                       job_queue_arn=os.environ['BATCH_CONVERTER_JOB_QUEUE_ARN'],
                                                       job_def_arn=os.environ['BATCH_CONVERTER_JOB_DEFINITION_ARN'],
                                                       command=mock.ANY,
                                                       environment=mock.ANY)
        mock_cw_put.assert_called_once_with(metric_name=MetricName.CONVERSION_REQUEST, metric_value=1)

    def test_enqueue_batch_job(self):
        expected_params = {
            'jobName': "test_job_name",
            'jobQueue': "test_job_queue",
            'jobDefinition': "test_job_definition",
            'containerOverrides': {
                'command': [],
                'environment': []
            }
        }
        expected_response = {
            'jobId': "test_id",
            'jobName': "test_job_name"
        }
        self.mock_batch_client.add_response('submit_job', expected_response, expected_params)
        self.mock_batch_client.activate()

        self.batch_handler._enqueue_batch_job("test_job_name", "test_job_queue", "test_job_definition", [], {})

    def test_get_batch_job_status(self):
        expected_params = {
            'jobs': ['123']
        }
        expected_response = {
            'jobs': [{
                'status': "FAILED",
                'jobName': "test_job_name",
                'jobId': "test_job_id",
                'jobQueue': "test_job_queue",
                'startedAt': 123,
                'jobDefinition': "test_job_definition"
            }]
        }
        self.mock_batch_client.add_response('describe_jobs', expected_response, expected_params)
        self.mock_batch_client.activate()

        status = self.batch_handler.get_batch_job_status('123')

        self.assertEqual(status, 'FAILED')

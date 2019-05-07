import uuid
from unittest import mock
from datetime import timedelta

from matrix.common import date
from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, RequestTableField
from matrix.common.request.request_tracker import RequestTracker, Subtask
from tests.unit import MatrixTestCaseUsingMockAWS
from matrix.common.aws.cloudwatch_handler import MetricName


class TestRequestTracker(MatrixTestCaseUsingMockAWS):

    @mock.patch("matrix.common.date.get_datetime_now")
    def setUp(self, mock_get_datetime_now):
        super(TestRequestTracker, self).setUp()
        self.stub_date = '2019-03-18T180907.136216Z'
        mock_get_datetime_now.return_value = self.stub_date

        self.request_id = str(uuid.uuid4())
        self.request_tracker = RequestTracker(self.request_id)
        self.dynamo_handler = DynamoHandler()

        self.create_test_request_table()

        self.dynamo_handler.create_request_table_entry(self.request_id, "test_format")

    def test_format(self):
        self.assertEqual(self.request_tracker.format, "test_format")

    def test_batch_job_id(self):
        self.assertEqual(self.request_tracker.batch_job_id, None)

        field_enum = RequestTableField.BATCH_JOB_ID
        self.dynamo_handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE,
                                                       self.request_id,
                                                       field_enum,
                                                       "123-123")
        self.assertEqual(self.request_tracker.batch_job_id, "123-123")

    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.get_batch_job_status")
    def test_batch_job_status(self, mock_get_job_status):
        mock_get_job_status.return_value = "FAILED"
        field_enum = RequestTableField.BATCH_JOB_ID
        self.dynamo_handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE,
                                                       self.request_id,
                                                       field_enum,
                                                       "123-123")

        self.assertEqual(self.request_tracker.batch_job_status, "FAILED")

    def test_creation_date(self):
        self.assertEqual(self.request_tracker.creation_date, self.stub_date)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_error(self, mock_cw_put):
        self.assertEqual(self.request_tracker.error, "")

        self.request_tracker.log_error("test error")
        self.assertEqual(self.request_tracker.error, "test error")
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST_ERROR, metric_value=1)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_request_table_entry")
    def test_initialize_request(self, mock_create_request_table_entry, mock_create_cw_metric):
        self.request_tracker.initialize_request("test_format")

        mock_create_request_table_entry.assert_called_once_with(self.request_id, "test_format")
        mock_create_cw_metric.assert_called_once()

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.increment_table_field")
    def test_expect_subtask_execution(self, mock_increment_table_field):
        self.request_tracker.expect_subtask_execution(Subtask.DRIVER)

        mock_increment_table_field.assert_called_once_with(DynamoTable.REQUEST_TABLE,
                                                           self.request_id,
                                                           RequestTableField.EXPECTED_DRIVER_EXECUTIONS,
                                                           1)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.increment_table_field")
    def test_complete_subtask_execution(self, mock_increment_table_field):
        self.request_tracker.complete_subtask_execution(Subtask.DRIVER)

        mock_increment_table_field.assert_called_once_with(DynamoTable.REQUEST_TABLE,
                                                           self.request_id,
                                                           RequestTableField.COMPLETED_DRIVER_EXECUTIONS,
                                                           1)

    def test_is_request_complete(self):
        self.assertFalse(self.request_tracker.is_request_complete())

        self.dynamo_handler.increment_table_field(DynamoTable.REQUEST_TABLE,
                                                  self.request_id,
                                                  RequestTableField.COMPLETED_CONVERTER_EXECUTIONS,
                                                  1)
        self.dynamo_handler.increment_table_field(DynamoTable.REQUEST_TABLE,
                                                  self.request_id,
                                                  RequestTableField.COMPLETED_QUERY_EXECUTIONS,
                                                  3)
        self.assertTrue(self.request_tracker.is_request_complete())

    def test_is_request_ready_for_conversion(self):
        self.assertFalse(self.request_tracker.is_request_ready_for_conversion())
        self.dynamo_handler.increment_table_field(DynamoTable.REQUEST_TABLE,
                                                  self.request_id,
                                                  RequestTableField.COMPLETED_QUERY_EXECUTIONS,
                                                  3)
        self.assertTrue(self.request_tracker.is_request_ready_for_conversion())

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_complete_request(self, mock_cw_put):
        duration = 1

        self.request_tracker.complete_request(duration)

        expected_calls = [
            mock.call(metric_name=MetricName.CONVERSION_COMPLETION, metric_value=1),
            mock.call(metric_name=MetricName.REQUEST_COMPLETION, metric_value=1),
            mock.call(metric_name=MetricName.DURATION, metric_value=duration, metric_dimensions=[
                {
                    'Name': "Output Format",
                    'Value': mock.ANY
                },
            ])
        ]
        mock_cw_put.assert_has_calls(expected_calls)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.log_error")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.creation_date",
                new_callable=mock.PropertyMock)
    def test_timeout(self, mock_creation_date, mock_log_error):
        # no timeout
        mock_creation_date.return_value = date.to_string(date.get_datetime_now() - timedelta(hours=11, minutes=59))
        self.assertFalse(self.request_tracker.timeout)

        # timeout
        mock_creation_date.return_value = date.to_string(date.get_datetime_now() - timedelta(hours=12, minutes=1))
        self.assertTrue(self.request_tracker.timeout)
        mock_log_error.assert_called_once()

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.set_table_field_with_value")
    def test_write_batch_job_id_to_db(self, mock_set_table_field_with_value):
        self.request_tracker.write_batch_job_id_to_db("123-123")
        mock_set_table_field_with_value.assert_called_once_with(DynamoTable.REQUEST_TABLE,
                                                                self.request_id,
                                                                RequestTableField.BATCH_JOB_ID,
                                                                "123-123")

import uuid
from unittest import mock

from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, StateTableField
from matrix.common.request.request_tracker import RequestTracker, Subtask
from tests.unit import MatrixTestCaseUsingMockAWS
from matrix.common.aws.cloudwatch_handler import MetricName


class TestRequestTracker(MatrixTestCaseUsingMockAWS):
    def setUp(self):
        super(TestRequestTracker, self).setUp()

        self.request_id = str(uuid.uuid4())
        self.request_tracker = RequestTracker(self.request_id)
        self.dynamo_handler = DynamoHandler()

        self.create_test_output_table()
        self.create_test_state_table()

        self.dynamo_handler.create_state_table_entry(self.request_id)
        self.dynamo_handler.create_output_table_entry(self.request_id, 1, "test_format")

    def test_format(self):
        self.assertEqual(self.request_tracker.format, "test_format")

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.num_bundles",
                new_callable=mock.PropertyMock)
    def test_num_bundles_interval(self, mock_num_bundles):
        mock_num_bundles.return_value = 0
        self.assertEqual(self.request_tracker.num_bundles_interval, "0-499")

        mock_num_bundles.return_value = 1
        self.assertEqual(self.request_tracker.num_bundles_interval, "0-499")

        mock_num_bundles.return_value = 500
        self.assertEqual(self.request_tracker.num_bundles_interval, "500-999")

        mock_num_bundles.return_value = 1234
        self.assertEqual(self.request_tracker.num_bundles_interval, "1000-1499")

    def test_error(self):
        self.assertEqual(self.request_tracker.error, "")

        self.dynamo_handler.write_request_error(self.request_id, "test error")
        self.assertEqual(self.request_tracker.error, "test error")

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_state_table_entry")
    def test_initialize_request(self, mock_create_state_table_entry, mock_create_cw_metric):
        self.request_tracker.initialize_request("test_format")

        mock_create_state_table_entry.assert_called_once_with(self.request_id)
        mock_create_cw_metric.assert_called_once()

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.increment_table_field")
    def test_expect_subtask_execution(self, mock_increment_table_field):
        self.request_tracker.expect_subtask_execution(Subtask.DRIVER)

        mock_increment_table_field.assert_called_once_with(DynamoTable.STATE_TABLE,
                                                           self.request_id,
                                                           StateTableField.EXPECTED_DRIVER_EXECUTIONS,
                                                           1)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.increment_table_field")
    def test_complete_subtask_execution(self, mock_increment_table_field):
        self.request_tracker.complete_subtask_execution(Subtask.DRIVER)

        mock_increment_table_field.assert_called_once_with(DynamoTable.STATE_TABLE,
                                                           self.request_id,
                                                           StateTableField.COMPLETED_DRIVER_EXECUTIONS,
                                                           1)

    def test_is_request_complete(self):
        self.assertFalse(self.request_tracker.is_request_complete())

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_CONVERTER_EXECUTIONS,
                                                  1)
        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_QUERY_EXECUTIONS,
                                                  3)
        self.assertTrue(self.request_tracker.is_request_complete())

    def test_is_request_ready_for_conversion(self):
        self.assertFalse(self.request_tracker.is_request_ready_for_conversion())
        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_QUERY_EXECUTIONS,
                                                  3)
        self.assertTrue(self.request_tracker.is_request_ready_for_conversion())

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.write_request_error")
    def test_log_error(self, mock_write_request_error, mock_cw_put):
        self.request_tracker.log_error("test error")
        mock_write_request_error.assert_called_once_with(self.request_id, "test error")
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST_ERROR, metric_value=1)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_complete_request(self, mock_cw_put):
        duration = 1

        self.request_tracker.complete_request(duration)

        expected_calls = [
            mock.call(metric_name=MetricName.REQUEST_COMPLETION, metric_value=1),
            mock.call(metric_name=MetricName.DURATION, metric_value=duration, metric_dimensions=[
                {
                    'Name': "Number of Bundles",
                    'Value': mock.ANY
                },
                {
                    'Name': "Output Format",
                    'Value': mock.ANY
                },
            ]),
            mock.call(metric_name=MetricName.DURATION, metric_value=duration, metric_dimensions=[
                {
                    'Name': "Number of Bundles",
                    'Value': mock.ANY
                },
            ]),
        ]
        mock_cw_put.assert_has_calls(expected_calls)

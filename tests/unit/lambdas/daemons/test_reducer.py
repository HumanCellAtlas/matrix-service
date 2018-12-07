import hashlib
from unittest import mock

from matrix.lambdas.daemons.reducer import Reducer
from matrix.common.aws.dynamo_handler import DynamoHandler
from matrix.common.aws.cloudwatch_handler import MetricName
from matrix.common.request.request_tracker import Subtask
from tests.unit import MatrixTestCaseUsingMockAWS


class TestReducer(MatrixTestCaseUsingMockAWS):
    def setUp(self):
        super(TestReducer, self).setUp()

        self.request_hash = hashlib.sha256().hexdigest()
        self.create_test_output_table()

        self.dynamo_handler = DynamoHandler()

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.schedule_matrix_conversion")
    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore.write_group_metadata")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    def test_run_zarr(self,
                      mock_complete_subtask_execution,
                      mock_write_group_metadata,
                      mock_schedule_matrix_conversion,
                      mock_cw_put):
        self.dynamo_handler.create_output_table_entry(self.request_hash, "zarr")

        self.reducer = Reducer(self.request_hash)
        self.reducer.run()

        self.assertEqual(mock_schedule_matrix_conversion.call_count, 0)
        mock_write_group_metadata.assert_called_once_with()
        mock_complete_subtask_execution.assert_called_once_with(Subtask.REDUCER)
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST_COMPLETION, metric_value=1)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.schedule_matrix_conversion")
    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore.write_group_metadata")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    def test_run_loom(self,
                      mock_complete_subtask_execution,
                      mock_write_group_metadata,
                      mock_schedule_matrix_conversion,
                      mock_cw_put):
        self.dynamo_handler.create_output_table_entry(self.request_hash, "loom")

        self.reducer = Reducer(self.request_hash)
        self.reducer.run()

        mock_schedule_matrix_conversion.assert_called_once_with("loom")
        mock_write_group_metadata.assert_called_once_with()
        mock_complete_subtask_execution.assert_called_once_with(Subtask.REDUCER)
        mock_cw_put.assert_called_once_with(metric_name=MetricName.CONVERSION_REQUEST, metric_value=1)

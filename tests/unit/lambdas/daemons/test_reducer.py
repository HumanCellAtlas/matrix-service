import hashlib
from unittest import mock

from matrix.lambdas.daemons.reducer import Reducer
from matrix.common.aws.dynamo_handler import DynamoHandler
from matrix.common.request.request_tracker import Subtask
from tests.unit import MatrixTestCaseUsingMockAWS


class TestReducer(MatrixTestCaseUsingMockAWS):
    def setUp(self):
        super(TestReducer, self).setUp()

        self.request_hash = hashlib.sha256().hexdigest()
        self.create_test_output_table()

        self.dynamo_handler = DynamoHandler()

    @mock.patch("matrix.lambdas.daemons.reducer.Reducer._schedule_matrix_conversion")
    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore.write_group_metadata")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    def test_run_zarr(self,
                      mock_complete_subtask_execution,
                      mock_write_group_metadata,
                      mock_schedule_matrix_conversion):
        self.dynamo_handler.create_output_table_entry(self.request_hash, "zarr")

        self.reducer = Reducer(self.request_hash)
        self.reducer.run()

        self.assertEqual(mock_schedule_matrix_conversion.call_count, 0)
        mock_write_group_metadata.assert_called_once_with()
        mock_complete_subtask_execution.assert_called_once_with(Subtask.REDUCER)

    @mock.patch("matrix.lambdas.daemons.reducer.Reducer._schedule_matrix_conversion")
    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore.write_group_metadata")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    def test_run_loom(self,
                      mock_complete_subtask_execution,
                      mock_write_group_metadata,
                      mock_schedule_matrix_conversion):
        self.dynamo_handler.create_output_table_entry(self.request_hash, "loom")

        self.reducer = Reducer(self.request_hash)
        self.reducer.run()

        mock_schedule_matrix_conversion.assert_called_once_with()
        mock_write_group_metadata.assert_called_once_with()
        mock_complete_subtask_execution.assert_called_once_with(Subtask.REDUCER)

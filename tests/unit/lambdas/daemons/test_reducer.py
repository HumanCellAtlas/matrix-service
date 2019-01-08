import hashlib
import uuid
from unittest import mock

from matrix.common import date
from matrix.lambdas.daemons.reducer import Reducer
from matrix.common.aws.dynamo_handler import DynamoHandler
from matrix.common.request.request_tracker import Subtask
from tests.unit import MatrixTestCaseUsingMockAWS


class TestReducer(MatrixTestCaseUsingMockAWS):
    def setUp(self):
        super(TestReducer, self).setUp()

        self.request_id = str(uuid.uuid4())
        self.request_hash = hashlib.sha256().hexdigest()
        self.create_test_output_table()

        self.dynamo_handler = DynamoHandler()

    @mock.patch("matrix.common.request.request_cache.RequestCache.creation_date",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.schedule_matrix_conversion")
    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore.write_group_metadata")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_request")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    def test_run_zarr(self,
                      mock_complete_subtask_execution,
                      mock_complete_request,
                      mock_write_group_metadata,
                      mock_schedule_matrix_conversion,
                      mock_creation_date):
        mock_creation_date.return_value = date.get_datetime_now(as_string=True)
        self.dynamo_handler.create_output_table_entry(self.request_hash, 1, "zarr")

        self.reducer = Reducer(self.request_id, self.request_hash)
        self.reducer.run()

        self.assertEqual(mock_schedule_matrix_conversion.call_count, 0)
        mock_write_group_metadata.assert_called_once_with()
        mock_complete_subtask_execution.assert_called_once_with(Subtask.REDUCER)
        mock_complete_request.assert_called_once_with(duration=mock.ANY)

    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.schedule_matrix_conversion")
    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore.write_group_metadata")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_request")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    def test_run_loom(self,
                      mock_complete_subtask_execution,
                      mock_complete_request,
                      mock_write_group_metadata,
                      mock_schedule_matrix_conversion):
        self.dynamo_handler.create_output_table_entry(self.request_hash, 1, "loom")

        self.reducer = Reducer(self.request_id, self.request_hash)
        self.reducer.run()

        mock_schedule_matrix_conversion.assert_called_once_with("loom")
        mock_write_group_metadata.assert_called_once_with()
        mock_complete_subtask_execution.assert_called_once_with(Subtask.REDUCER)
        self.assertEqual(mock_complete_request.call_count, 0)

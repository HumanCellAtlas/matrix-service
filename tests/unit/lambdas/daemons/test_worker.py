import hashlib
import os
import uuid

from pandas import DataFrame
from unittest import mock

from matrix.lambdas.daemons.worker import Worker
from matrix.common.aws.lambda_handler import LambdaName
from matrix.common.request.request_tracker import Subtask
from tests import test_bundle_spec
from tests.unit import MatrixTestCaseUsingMockAWS


class TestWorker(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestWorker, self).setUp()

        self.state_table_name = os.environ['DYNAMO_STATE_TABLE_NAME']
        self.output_table_name = os.environ['DYNAMO_OUTPUT_TABLE_NAME']
        self.request_id = str(uuid.uuid4())
        self.request_hash = hashlib.sha256().hexdigest()
        self.create_test_state_table()
        self.create_test_output_table()
        self.format_string = "zarr"
        self.worker_chunk_spec = [{
            "bundle_uuid": test_bundle_spec['uuid'],
            "bundle_version": test_bundle_spec['version'],
            "start_row": 100,
            "num_rows": 6500
        }]
        self.worker = Worker(self.request_id, self.request_hash)

    @mock.patch('matrix.common.zarr.s3_zarr_store.S3ZarrStore.write_from_pandas_dfs')
    @mock.patch('matrix.common.zarr.dss_zarr_store.DSSZarrStore.__init__')
    @mock.patch('zarr.group')
    @mock.patch('matrix.lambdas.daemons.worker.convert_dss_zarr_root_to_subset_pandas_dfs')
    @mock.patch('matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution')
    @mock.patch('matrix.common.request.request_tracker.RequestTracker.is_reducer_ready')
    @mock.patch('matrix.common.zarr.s3_zarr_store.S3ZarrStore.write_column_data')
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_run(self,
                 mock_lambda_handler_invoke,
                 mock_write_column_data,
                 mock_is_reducer_ready,
                 mock_complete_subtask_execution,
                 mock_df_conversion,
                 mock_zarr_group,
                 mock_dss_zarr_store,
                 mock_write_to_s3):
        mock_dss_zarr_store.return_value = None
        mock_write_to_s3.return_value = None
        mock_zarr_group.return_value = None
        mock_df_conversion.return_value = (DataFrame(), DataFrame())
        mock_is_reducer_ready.return_value = True
        self.worker.run(self.worker_chunk_spec)
        mock_lambda_handler_invoke.assert_called_once_with(LambdaName.REDUCER, {
            'request_id': self.request_id,
            'request_hash': self.request_hash
        })
        mock_complete_subtask_execution.assert_called_once_with(Subtask.WORKER)

    def test_parse_worker_chunk_spec(self):
        self.worker._parse_worker_chunk_spec(self.worker_chunk_spec)
        self.assertEqual(self.worker._bundle_uuids,
                         [s['bundle_uuid'] for s in self.worker_chunk_spec])
        self.assertEqual(self.worker._bundle_versions,
                         [s['bundle_version'] for s in self.worker_chunk_spec])
        self.assertEqual(self.worker._input_start_rows, [100])
        self.assertEqual(self.worker._input_end_rows, [6600])

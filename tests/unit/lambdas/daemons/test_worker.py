import os
import uuid

import boto3
from unittest import mock

from ... import MatrixTestCaseUsingMockAWS
from ... import test_bundle_spec
from matrix.lambdas.daemons.worker import Worker
from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import StateTableField
from matrix.common.dynamo_handler import DynamoTable
from matrix.common.lambda_handler import LambdaName


class TestWorker(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestWorker, self).setUp()

        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self.state_table_name = os.environ['DYNAMO_STATE_TABLE_NAME']
        self.output_table_name = os.environ['DYNAMO_OUTPUT_TABLE_NAME']
        self.request_id = str(uuid.uuid4())
        self.create_test_state_table(self.dynamo)
        self.create_test_output_table(self.dynamo)
        self.handler = DynamoHandler()
        self.format_string = "zarr"
        self.worker_chunk_spec = [{
            "bundle_uuid": test_bundle_spec['uuid'],
            "bundle_version": test_bundle_spec['version'],
            "start_row": 100,
            "num_rows": 6500
        }]
        self.worker = Worker(self.request_id)

    @mock.patch('matrix.common.s3_zarr_store.S3ZarrStore.write_from_pandas_dfs')
    @mock.patch('matrix.common.dss_zarr_store.DSSZarrStore.__init__')
    @mock.patch('zarr.group')
    @mock.patch('matrix.lambdas.daemons.worker.convert_dss_zarr_root_to_subset_pandas_dfs')
    @mock.patch('matrix.lambdas.daemons.worker.Worker._check_if_all_workers_and_mappers_for_request_are_complete')
    @mock.patch('matrix.common.dynamo_handler.DynamoHandler.increment_table_field')
    @mock.patch('matrix.common.s3_zarr_store.S3ZarrStore.write_column_data')
    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_run(self, mock_lambda_handler_invoke, mock_write_column_data, mock_increment_dynamo, mock_is_complete,
                 mock_df_conversion, mock_zarr_group, mock_dss_zarr_store, mock_write_to_s3):
        mock_dss_zarr_store.return_value = None
        mock_write_to_s3.return_value = None
        mock_zarr_group.return_value = None
        mock_df_conversion.return_value = (None, None)
        mock_is_complete.return_value = True
        self.worker.run(self.format_string, self.worker_chunk_spec)
        mock_lambda_handler_invoke.assert_called_once_with(LambdaName.REDUCER, {
            'request_id': self.request_id, 'format': self.format_string})

    def test_parse_worker_chunk_spec(self):
        self.worker._parse_worker_chunk_spec(self.worker_chunk_spec)
        self.assertEqual(self.worker._bundle_uuids,
                         [s['bundle_uuid'] for s in self.worker_chunk_spec])
        self.assertEqual(self.worker._bundle_versions,
                         [s['bundle_version'] for s in self.worker_chunk_spec])
        self.assertEqual(self.worker._input_start_rows, [100])
        self.assertEqual(self.worker._input_end_rows, [6600])

    def test_check_if_all_workers_and_mappers_for_request_are_complete_returns_false(self):
        self.handler.create_state_table_entry(self.request_id, num_bundles=2)
        complete = self.worker._check_if_all_workers_and_mappers_for_request_are_complete(self.request_id)
        self.assertEqual(complete, False)

        field_enum = StateTableField.EXPECTED_WORKER_EXECUTIONS
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_enum, 5)
        field_enum = StateTableField.COMPLETED_MAPPER_EXECUTIONS
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_enum, 2)

        complete = self.worker._check_if_all_workers_and_mappers_for_request_are_complete(self.request_id)
        self.assertEqual(complete, False)

    def test_check_if_all_workers_and_mappers_for_request_are_complete_returns_true(self):
        self.handler.create_state_table_entry(self.request_id, num_bundles=2)
        complete = self.worker._check_if_all_workers_and_mappers_for_request_are_complete(self.request_id)
        self.assertEqual(complete, False)

        field_enum = StateTableField.EXPECTED_WORKER_EXECUTIONS
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_enum, 5)
        field_enum = StateTableField.COMPLETED_MAPPER_EXECUTIONS
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_enum, 2)
        field_enum = StateTableField.COMPLETED_WORKER_EXECUTIONS
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_enum, 5)

        complete = self.worker._check_if_all_workers_and_mappers_for_request_are_complete(self.request_id)
        self.assertEqual(complete, True)

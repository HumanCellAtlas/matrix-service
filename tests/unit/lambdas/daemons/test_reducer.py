import os
import uuid
from unittest import mock

import boto3

from matrix.lambdas.daemons.reducer import Reducer
from matrix.common.dynamo_handler import DynamoHandler, DynamoTable, StateTableField
from tests.unit import MatrixTestCaseUsingMockAWS


class TestReducer(MatrixTestCaseUsingMockAWS):
    def setUp(self):
        super(TestReducer, self).setUp()

        self.request_id = str(uuid.uuid4())
        self.create_test_output_table(boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION']))

        self.dynamo_handler = DynamoHandler()

    @mock.patch("matrix.lambdas.daemons.reducer.Reducer._schedule_matrix_conversion")
    @mock.patch("matrix.common.s3_zarr_store.S3ZarrStore.write_group_metadata")
    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.increment_table_field")
    def test_run_zarr(self,
                      mock_dynamo_increment_table_field,
                      mock_write_group_metadata,
                      mock_schedule_matrix_conversion):
        self.dynamo_handler.create_output_table_entry(self.request_id, "zarr")

        self.reducer = Reducer(self.request_id)
        self.reducer.run()

        self.assertEqual(mock_schedule_matrix_conversion.call_count, 0)
        mock_write_group_metadata.assert_called_once_with()
        mock_dynamo_increment_table_field.assert_called_once_with(DynamoTable.STATE_TABLE,
                                                                  self.request_id,
                                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS,
                                                                  1)

    @mock.patch("matrix.lambdas.daemons.reducer.Reducer._schedule_matrix_conversion")
    @mock.patch("matrix.common.s3_zarr_store.S3ZarrStore.write_group_metadata")
    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.increment_table_field")
    def test_run_loom(self,
                      mock_dynamo_increment_table_field,
                      mock_write_group_metadata,
                      mock_schedule_matrix_conversion):
        self.dynamo_handler.create_output_table_entry(self.request_id, "loom")

        self.reducer = Reducer(self.request_id)
        self.reducer.run()

        mock_schedule_matrix_conversion.assert_called_once_with()
        mock_write_group_metadata.assert_called_once_with()
        mock_dynamo_increment_table_field.assert_called_once_with(DynamoTable.STATE_TABLE,
                                                                  self.request_id,
                                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS,
                                                                  1)

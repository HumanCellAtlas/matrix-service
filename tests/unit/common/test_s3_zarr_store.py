import os
import uuid

import boto3
from pandas import DataFrame
from unittest import mock

from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.s3_zarr_store import S3ZarrStore
from .. import MatrixTestCaseUsingMockAWS
from matrix.common.dynamo_handler import OutputTableField
from matrix.common.dynamo_handler import DynamoTable


class TestZarrS3Store(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestZarrS3Store, self).setUp()

        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self.state_table_name = os.environ['DYNAMO_STATE_TABLE_NAME']
        self.output_table_name = os.environ['DYNAMO_OUTPUT_TABLE_NAME']
        self.create_test_state_table(self.dynamo)
        self.create_test_output_table(self.dynamo)

        self.request_id = str(uuid.uuid4())
        self.dynamo_handler = DynamoHandler()
        self.dynamo_handler.create_output_table_entry(self.request_id)

        self.s3_zarr_store = S3ZarrStore(self.request_id)

    def test_get_output_row_boundaries(self):
        start_row_idx, end_row_idx = self.s3_zarr_store._get_output_row_boundaries(6000)
        self.assertEqual(start_row_idx, 0)
        self.assertEqual(end_row_idx, 6000)

        start_row_idx, end_row_idx = self.s3_zarr_store._get_output_row_boundaries(5000)
        self.assertEqual(start_row_idx, 6000)
        self.assertEqual(end_row_idx, 11000)

    def test_get_output_chunk_boundaries(self):
        start_chunk_idx, end_chunk_idx = self.s3_zarr_store._get_output_chunk_boundaries(0, 6000)
        self.assertEqual(start_chunk_idx, 0)
        self.assertEqual(end_chunk_idx, 2)

        start_chunk_idx, end_chunk_idx = self.s3_zarr_store._get_output_chunk_boundaries(6000, 10000)
        self.assertEqual(start_chunk_idx, 2)
        self.assertEqual(end_chunk_idx, 4)

        start_chunk_idx, end_chunk_idx = self.s3_zarr_store._get_output_chunk_boundaries(10000, 15000)
        self.assertEqual(start_chunk_idx, 3)
        self.assertEqual(end_chunk_idx, 5)

    @mock.patch("matrix.common.s3_zarr_store.S3ZarrStore._write_row_data_to_results_chunk")
    def test_write_from_pandas_dfs_from_zero_num_rows(self, mock_write_row_data):
        exp_df = DataFrame()
        qc_df = DataFrame()
        num_rows = 5000
        self.s3_zarr_store.write_from_pandas_dfs(exp_df, qc_df, num_rows)
        input_bounds_one = (0, 3000)
        input_bounds_two = (3000, 5000)
        output_bounds_one = (0, 3000)
        output_bounds_two = (0, 2000)
        expected_calls = [
            mock.call(0, input_bounds_one, output_bounds_one),
            mock.call(1, input_bounds_two, output_bounds_two),
        ]
        mock_write_row_data.assert_has_calls(expected_calls)
        self.assertEqual(mock_write_row_data.call_count, 2)

    @mock.patch("matrix.common.s3_zarr_store.S3ZarrStore._write_row_data_to_results_chunk")
    def test_write_from_pandas_dfs_from_preexisting_rows(self, mock_write_row_data):
        field_value = OutputTableField.ROW_COUNT.value
        self.dynamo_handler.increment_table_field(DynamoTable.OUTPUT_TABLE, self.request_id, field_value, 50)
        exp_df = DataFrame()
        qc_df = DataFrame()
        num_rows = 5000
        self.s3_zarr_store.write_from_pandas_dfs(exp_df, qc_df, num_rows)
        input_bounds_one = (0, 2950)
        input_bounds_two = (2950, 5000)
        output_bounds_one = (50, 3000)
        output_bounds_two = (0, 2050)
        expected_calls = [
            mock.call(0, input_bounds_one, output_bounds_one),
            mock.call(1, input_bounds_two, output_bounds_two),
        ]
        mock_write_row_data.assert_has_calls(expected_calls)
        self.assertEqual(mock_write_row_data.call_count, 2)

import json
import os
import uuid

from pandas import DataFrame, Series
from unittest import mock

from matrix.common.constants import ZarrayName
from matrix.common.exceptions import MatrixException
from matrix.common.aws.dynamo_handler import DynamoHandler
from matrix.common.zarr.s3_zarr_store import S3ZarrStore, ZARR_OUTPUT_CONFIG
from tests.unit import MatrixTestCaseUsingMockAWS
from matrix.common.aws.dynamo_handler import OutputTableField
from matrix.common.aws.dynamo_handler import DynamoTable


class TestS3ZarrStore(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestS3ZarrStore, self).setUp()

        self.s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        self.state_table_name = os.environ['DYNAMO_STATE_TABLE_NAME']
        self.output_table_name = os.environ['DYNAMO_OUTPUT_TABLE_NAME']
        self.create_test_state_table()
        self.create_test_output_table()

        self.request_id = str(uuid.uuid4())
        self.dynamo_handler = DynamoHandler()
        self.dynamo_handler.create_output_table_entry(self.request_id, 1, "zarr")

        test_array = [0] * ZARR_OUTPUT_CONFIG['cells_per_chunk']
        exp_df = DataFrame(data=test_array, index=test_array)
        qc_df = DataFrame({'cell_metadata_numeric': Series(test_array, dtype="float32"),
                           'cell_metadata_string': Series(test_array, dtype="object")})
        self.s3_zarr_store = S3ZarrStore(self.request_id, exp_df, qc_df)

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

    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore._write_row_data_to_results_chunk")
    def test_write_from_pandas_dfs_from_zero_num_rows(self, mock_write_row_data):
        num_rows = 5000
        self.s3_zarr_store.write_from_pandas_dfs(num_rows)
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

    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore._write_row_data_to_results_chunk")
    def test_write_from_pandas_dfs_from_preexisting_rows(self, mock_write_row_data):
        field_enum = OutputTableField.ROW_COUNT
        self.dynamo_handler.increment_table_field(DynamoTable.OUTPUT_TABLE, self.request_id, field_enum, 50)
        num_rows = 5000
        self.s3_zarr_store.write_from_pandas_dfs(num_rows)
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

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore._write_zarray_metadata")
    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore._write_zgroup_metadata")
    def test_write_group_metadata(self,
                                  mock_write_zgroup_metadata,
                                  mock_write_zarray_metadata,
                                  mock_dynamo_get_table_item):
        num_rows = 0
        mock_dynamo_get_table_item.return_value = {OutputTableField.ROW_COUNT.value: num_rows}

        self.s3_zarr_store.write_group_metadata()

        mock_write_zgroup_metadata.assert_called_once_with()

        expected_calls = [
            mock.call(ZarrayName.EXPRESSION, num_rows),
            mock.call(ZarrayName.CELL_METADATA_NUMERIC, num_rows),
            mock.call(ZarrayName.CELL_METADATA_STRING, num_rows),
            mock.call(ZarrayName.CELL_ID, num_rows),
        ]
        mock_write_zarray_metadata.assert_has_calls(expected_calls)
        self.assertEqual(mock_write_zarray_metadata.call_count, 4)

    def test_write_zgroup_metadata(self):
        self.create_s3_results_bucket()
        self.s3_zarr_store._write_zgroup_metadata()

        s3_zgroup_location = f"s3://{self.s3_results_bucket}/{self.request_id}.zarr/.zgroup"
        data = json.loads(self.s3_zarr_store.s3_file_system.open(s3_zgroup_location, "rb").read())
        self.assertEqual(data['zarr_format'], 2)

    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore._fill_value")
    @mock.patch("matrix.common.zarr.s3_zarr_store.S3ZarrStore._get_zarray_column_count")
    def test_write_zarray_metadata(self, mock_get_zarray_column_count, mock_fill_value):
        zarray = ZarrayName.EXPRESSION
        row_count = 0
        mock_get_zarray_column_count.return_value = 1
        mock_fill_value.return_value = ""

        self.create_s3_results_bucket()
        self.s3_zarr_store._write_zarray_metadata(zarray, row_count)

        s3_zarray_location = f"s3://{self.s3_results_bucket}/{self.request_id}.zarr/{zarray.value}/.zarray"
        data = json.loads(self.s3_zarr_store.s3_file_system.open(s3_zarray_location, "rb").read())

        self.assertEqual(data['chunks'], [ZARR_OUTPUT_CONFIG['cells_per_chunk'], 1])
        self.assertEqual(data['compressor'], ZARR_OUTPUT_CONFIG['compressor'].get_config())
        self.assertEqual(data['dtype'], ZARR_OUTPUT_CONFIG['dtypes'][zarray.value])
        self.assertEqual(data['fill_value'], "")
        self.assertEqual(data['filters'], None)
        self.assertEqual(data['order'], ZARR_OUTPUT_CONFIG['order'])
        self.assertEqual(data['shape'], [row_count, 1])
        self.assertEqual(data['zarr_format'], 2)

    @mock.patch("matrix.common.aws.dynamo_lock.DynamoLock.__exit__")
    @mock.patch("matrix.common.aws.dynamo_lock.DynamoLock.__enter__")
    @mock.patch("zarr.storage.default_compressor.decode")
    @mock.patch("tests.unit.common.zarr.test_s3_zarr_store.TestFileStub.write")
    @mock.patch("s3fs.S3FileSystem.open")
    def test_write_row_data_to_results_chunk(self,
                                             mock_s3_open,
                                             mock_write,
                                             mock_decode,
                                             mock_lock_enter,
                                             mock_lock_exit):
        cells_per_chunk = ZARR_OUTPUT_CONFIG['cells_per_chunk']
        mock_decode.return_value = None
        mock_s3_open.return_value = TestFileStub()

        self.s3_zarr_store._write_row_data_to_results_chunk(0, (0, cells_per_chunk), (0, cells_per_chunk))

        self.assertEqual(mock_write.call_count, 4)

    @mock.patch("json.loads")
    @mock.patch("s3fs.S3FileSystem.open")
    def test_get_zarray_column_count(self, mock_s3_open, mock_json_loads):
        cases = {
            ZarrayName.EXPRESSION: 1,
            ZarrayName.CELL_METADATA_NUMERIC: 2,
            ZarrayName.CELL_METADATA_STRING: 3,
            ZarrayName.CELL_ID: 0
        }

        for case in cases:
            with self.subTest(f"{case.value} zarray"):
                mock_json_loads.return_value = {'chunks': [cases[case]]}
                column_count = self.s3_zarr_store._get_zarray_column_count(case)
                self.assertEqual(column_count, cases[case])

        with self.assertRaises(MatrixException):
            self.s3_zarr_store._get_zarray_column_count(ZarrayName.GENE_ID)


class TestFileStub:
    def read(self):
        return ""

    def write(self, data):
        return

import os
import math

import s3fs
import zarr
from pandas import DataFrame

from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import DynamoTable
from matrix.common.dynamo_handler import OutputTableField


class ZarrToS3Writer():

    def __init__(self, cells_per_chunk=3000, compressor=zarr.storage.default_compressor):
        self.dynamo_handler = DynamoHandler()
        self.results_bucket = os.environ["S3_RESULTS_BUCKET"]
        self.s3_file_system = s3fs.S3FileSystem(anon=False)
        self.cells_per_chunk = cells_per_chunk
        self.compressor = compressor
        self.dtypes = {
            "data": "<f4",
            "cell_name": "<U64",
            "qc_values": "<f4"
        }
        self.order = "C"

    def run(self, request_id: str, filtered_exp_df: DataFrame, filtered_qc_df: DataFrame):
        """
        Method to write filtered pandas matrices to respective location in s3 zarr store
        Params:
            request_id: unique id of filter merge job
            filtered_exp_df: filtered expression pandas DataFrame
            filtered_qc_df: filtered qc values pandas DataFrame
        """
        start_chunk_idx, end_chunk_idx = self._get_output_row_chunk_idxs(request_id, filtered_exp_df.shape[0])
        # STILL A LOT TO IMPLEMENT HERE AND TEST. SHELL OF A METHOD

    def _get_output_row_chunk_idxs(self, request_id: str, nrows: int):
        """
        Get the start and end rows in the output table that we should write a filtered result to.
        Params:
            request_id: unique id of filter merge job
            nrows: number of rows for current chunk
        Output:
            Tuple:
                start_chunk_idx: (int) start row in output table to begin writing
                end_chunk_idx: (int) end row in output table at which to end writing
        """
        field_value = OutputTableField.ROW_COUNT.value
        start, end = self.dynamo_handler.increment_table_field(DynamoTable.OUTPUT_TABLE, request_id, field_value, nrows)
        start_chunk_idx = math.floor(start / self.cells_per_chunk)
        end_chunk_idx = math.ceil(end / self.cells_per_chunk)
        return start_chunk_idx, end_chunk_idx

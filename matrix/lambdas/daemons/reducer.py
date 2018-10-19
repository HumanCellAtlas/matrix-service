import json
import os

import numpy
import s3fs
import zarr

from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import DynamoTable
from matrix.common.dynamo_handler import StateTableField
from matrix.common.dynamo_handler import OutputTableField


class Reducer:
    # TODO: Move this to S3 Handler class
    ZARR_OUTPUT_CONFIG = {
        "cells_per_chunk": 3000,
        "compressor": zarr.storage.default_compressor,
        "dtypes": {
            "expression": "<f4",
            "cell_id": "<U64",
            "cell_metadata_numeric": "<f4",
            "cell_metadata_string": "<U64"
        },
        "order": "C"
    }

    def __init__(self, request_id: str, format: str):
        print(f"Reducer created: {request_id}, {format}")
        self.request_id = request_id
        self.format = format
        self.s3_results_bucket = os.environ['S3_RESULTS_BUCKET']

        self.dynamo_handler = DynamoHandler()

    def run(self):
        """
        Sequentially write all partial results from Worker lambdas to resultant expression matrix.
        """
        print("Running reducer")
        s3 = s3fs.S3FileSystem(anon=False)
        s3_results_prefix = f"s3://{self.s3_results_bucket}/{self.request_id}.zarr"

        # Write the zgroup file, which is very simple
        zgroup_key = f"{s3_results_prefix}/.zgroup"
        s3.open(zgroup_key, 'wb').write(json.dumps({"zarr_format": 2}).encode())

        num_genes = json.loads(s3.open(
            f"{s3_results_prefix}/gene_id/.zarray", 'rb').read())["chunks"][0]
        num_numeric_cell_metadata = json.loads(s3.open(
            f"{s3_results_prefix}/cell_metadata_numeric_name/.zarray", 'rb').read())["chunks"][0]
        num_string_cell_metadata = json.loads(s3.open(
            f"{s3_results_prefix}/cell_metadata_string_name/.zarray", 'rb').read())["chunks"][0]

        ncols = {"expression": int(num_genes), "cell_metadata_numeric": int(num_numeric_cell_metadata),
                 "cell_metadata_string": int(num_string_cell_metadata), "cell_id": 0}
        num_rows, num_rows = self.dynamo_handler.increment_table_field(DynamoTable.OUTPUT_TABLE,
                                                                       self.request_id,
                                                                       OutputTableField.ROW_COUNT,
                                                                       0)

        for dset in ["expression", "cell_metadata_numeric", "cell_metadata_string", "cell_id"]:
            zarray_key = f"{s3_results_prefix}/{dset}/.zarray"

            chunks = [Reducer.ZARR_OUTPUT_CONFIG["cells_per_chunk"]]
            shape = [int(num_rows)]
            if ncols[dset]:
                chunks.append(ncols[dset])
                shape.append(ncols[dset])

            zarray = {
                "chunks": chunks,
                "compressor": Reducer.ZARR_OUTPUT_CONFIG["compressor"].get_config(),
                "dtype": Reducer.ZARR_OUTPUT_CONFIG["dtypes"][dset],
                "fill_value": self._fill_value(numpy.dtype(Reducer.ZARR_OUTPUT_CONFIG["dtypes"][dset])),
                "filters": None,
                "order": Reducer.ZARR_OUTPUT_CONFIG["order"],
                "shape": shape,
                "zarr_format": 2
            }
            s3.open(zarray_key, 'wb').write(json.dumps(zarray).encode())

        if self.format is not "zarr":
            self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                      self.request_id,
                                                      StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value,
                                                      1)

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS,
                                                  1)

    def _fill_value(self, dtype):
        if dtype.kind == 'f':
            return float(0)
        elif dtype.kind == 'i':
            return 0
        elif dtype.kind == 'U':
            return ""

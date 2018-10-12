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
            "data": "<f4",
            "cell_name": "<U64",
            "qc_values": "<f4"
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
            f"{s3_results_prefix}/gene_name/.zarray", 'rb').read())["chunks"][0]
        num_qcs = json.loads(s3.open(
            f"{s3_results_prefix}/qc_names/.zarray", 'rb').read())["chunks"][0]
        ncols = {"data": int(num_genes), "qc_values": int(num_qcs), "cell_name": 0}
        num_rows, num_rows = self.dynamo_handler.increment_table_field(DynamoTable.OUTPUT_TABLE,
                                                                       self.request_id,
                                                                       OutputTableField.ROW_COUNT.value,
                                                                       0)

        for dset in ["data", "qc_values", "cell_name"]:
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

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS.value,
                                                  1)

    def _fill_value(self, dtype):
        if dtype.kind == 'f':
            return float(0)
        elif dtype.kind == 'i':
            return 0
        elif dtype.kind == 'U':
            return ""

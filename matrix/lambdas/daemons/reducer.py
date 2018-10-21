import json
import os

import numpy
import s3fs
import zarr
import boto3

from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import DynamoTable
from matrix.common.dynamo_handler import StateTableField
from matrix.common.dynamo_handler import OutputTableField


class Reducer:
    # TODO: Move this to S3 Handler class
    # TODO: Add tests to reducer
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

    def __init__(self, request_id: str):
        print(f"Reducer created: {request_id}, {format}")
        self.dynamo_handler = DynamoHandler()
        self.batch = boto3.client('batch')
        self.request_id = request_id
        item = self.dynamo_handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_id)
        self.format = item[OutputTableField.FORMAT.value]
        self.s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']

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

        if self.format != "zarr":
            self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                      self.request_id,
                                                      StateTableField.EXPECTED_CONVERTER_EXECUTIONS,
                                                      1)
            self._schedule_matrix_conversion()

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS,
                                                  1)

    def _schedule_matrix_conversion(self):
        # TODO TEST THIS WHEN REDUCER TESTS ARE ADDED
        source_zarr_path = f"s3://{self.s3_results_bucket}/{self.request_id}.zarr"
        target_path = f"s3://{self.s3_results_bucket}/{self.request_id}.{self.format}"
        job_queue_arn = "arn:aws:batch:us-east-1:861229788715:job-queue/dcp-matrix-converter-queue-dev"
        job_def_arn = "arn:aws:batch:us-east-1:861229788715:job-definition/dcp-matrix-converter-job-definition-dev"
        command = ['python3', '/matrix_converter.py', self.request_id, source_zarr_path, target_path, self.format]
        environment = {
            'DEPLOYMENT_STAGE': self.deployment_stage,
            'DYNAMO_STATE_TABLE_NAME': DynamoTable.STATE_TABLE.value,
            'DYNAMO_OUTPUT_TABLE_NAME': DynamoTable.OUTPUT_TABLE.value
        }
        job_name = "-".join([
            "conversion", self.deployment_stage, self.request_id, self.format])
        self._enqueue_batch_job(queue_arn=job_queue_arn,
                                job_name=job_name,
                                job_def_arn=job_def_arn,
                                command=command,
                                environment=environment)

    def _enqueue_batch_job(self, queue_arn, job_name, job_def_arn, command, environment):
        # TODO TEST THIS WHEN REDUCER TESTS ARE ADDED
        job = self.batch.submit_job(
            jobName=job_name,
            jobQueue=queue_arn,
            jobDefinition=job_def_arn,
            containerOverrides={
                'command': command,
                'environment': [dict(name=k, value=v) for k, v in environment.items()]
            }
        )
        print(f"Enqueued job {job_name} [{job['jobId']}] using job definition {job_def_arn}:")
        return job['jobId']

    def _fill_value(self, dtype):
        if dtype.kind == 'f':
            return float(0)
        elif dtype.kind == 'i':
            return 0
        elif dtype.kind == 'U':
            return ""

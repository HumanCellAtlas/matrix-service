import os

import boto3

from matrix.common.constants import MatrixFormat
from matrix.common.dynamo_handler import DynamoHandler, DynamoTable, StateTableField, OutputTableField
from matrix.common.s3_zarr_store import S3ZarrStore
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class Reducer:
    def __init__(self, request_id: str):
        Logging.set_correlation_id(logger, value=request_id)

        self.request_id = request_id
        self.s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']

        self.batch = boto3.client('batch')
        self.dynamo_handler = DynamoHandler()

        item = self.dynamo_handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_id)
        self.format = item[OutputTableField.FORMAT.value]

    def run(self):
        """
        Write resultant expression matrix zarr metadata in S3 after Workers complete.
        """
        logger.debug(f"Reducer running with parameters: format={self.format}")

        s3_zarr_store = S3ZarrStore(self.request_id)
        s3_zarr_store.write_group_metadata()

        if self.format != MatrixFormat.ZARR.value:
            self._schedule_matrix_conversion()

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS,
                                                  1)

    def _schedule_matrix_conversion(self):
        # TODO: write tests and clean up
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
        # TODO: write tests and clean up
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

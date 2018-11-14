import os

import boto3

from matrix.common.constants import MatrixFormat
from matrix.common.dynamo_handler import DynamoTable
from matrix.common.logging import Logging
from matrix.common.request_tracker import RequestTracker, Subtask
from matrix.common.s3_zarr_store import S3ZarrStore

logger = Logging.get_logger(__name__)


class Reducer:
    def __init__(self, request_hash: str):
        Logging.set_correlation_id(logger, value=request_hash)

        self.request_hash = request_hash
        self.s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']

        self.batch = boto3.client('batch')
        self.request_tracker = RequestTracker(self.request_hash)

    def run(self):
        """
        Write resultant expression matrix zarr metadata in S3 after Workers complete.
        """
        logger.debug(f"Reducer running with parameters: None")

        s3_zarr_store = S3ZarrStore(self.request_hash)
        s3_zarr_store.write_group_metadata()

        if self.request_tracker.format != MatrixFormat.ZARR.value:
            self._schedule_matrix_conversion()

        self.request_tracker.complete_subtask_execution(Subtask.REDUCER)

    def _schedule_matrix_conversion(self):
        # TODO: write tests and clean up
        format = self.request_tracker.format
        source_zarr_path = f"s3://{self.s3_results_bucket}/{self.request_hash}.zarr"
        target_path = f"s3://{self.s3_results_bucket}/{self.request_hash}.{format}"
        job_queue_arn = f"arn:aws:batch:us-east-1:861229788715:" \
                        f"job-queue/dcp-matrix-converter-queue-{self.deployment_stage}"
        job_def_arn = f"arn:aws:batch:us-east-1:861229788715:" \
                      f"job-definition/dcp-matrix-converter-job-definition-{self.deployment_stage}"
        command = ['python3', '/matrix_converter.py', self.request_hash, source_zarr_path, target_path, format]
        environment = {
            'DEPLOYMENT_STAGE': self.deployment_stage,
            'DYNAMO_STATE_TABLE_NAME': DynamoTable.STATE_TABLE.value,
            'DYNAMO_OUTPUT_TABLE_NAME': DynamoTable.OUTPUT_TABLE.value,
            'DYNAMO_CACHE_TABLE_NAME': DynamoTable.CACHE_TABLE.value
        }
        job_name = "-".join([
            "conversion", self.deployment_stage, self.request_hash, format])
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

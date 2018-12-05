import os

import boto3

from matrix.common.aws.dynamo_handler import DynamoTable
from matrix.common.constants import MatrixFormat
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class BatchHandler:
    def __init__(self, request_hash):
        Logging.set_correlation_id(logger, value=request_hash)

        self.request_hash = request_hash

        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']
        self.s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        self.job_queue_arn = os.environ['BATCH_CONVERTER_JOB_QUEUE_ARN']
        self.job_def_arn = os.environ['BATCH_CONVERTER_JOB_DEFINITION_ARN']

        self._client = boto3.client("batch", region_name=os.environ['AWS_DEFAULT_REGION'])

    def schedule_matrix_conversion(self, format):
        job_name = "-".join(["conversion",
                             self.deployment_stage,
                             self.request_hash,
                             format])

        is_compressed = format == MatrixFormat.CSV.value or format == MatrixFormat.MTX.value
        source_zarr_path = f"s3://{self.s3_results_bucket}/{self.request_hash}.zarr"
        target_path = f"s3://{self.s3_results_bucket}/{self.request_hash}.{format}" + (".zip" if is_compressed else "")
        command = ['python3', '/matrix_converter.py', self.request_hash, source_zarr_path, target_path, format]

        environment = {
            'DEPLOYMENT_STAGE': self.deployment_stage,
            'DYNAMO_STATE_TABLE_NAME': DynamoTable.STATE_TABLE.value,
            'DYNAMO_OUTPUT_TABLE_NAME': DynamoTable.OUTPUT_TABLE.value,
            'DYNAMO_CACHE_TABLE_NAME': DynamoTable.CACHE_TABLE.value
        }

        self._enqueue_batch_job(job_name=job_name,
                                job_queue_arn=self.job_queue_arn,
                                job_def_arn=self.job_def_arn,
                                command=command,
                                environment=environment)

    def _enqueue_batch_job(self, job_name, job_queue_arn, job_def_arn, command, environment):
        job = self._client.submit_job(
            jobName=job_name,
            jobQueue=job_queue_arn,
            jobDefinition=job_def_arn,
            containerOverrides={
                'command': command,
                'environment': [dict(name=k, value=v) for k, v in environment.items()]
            }
        )
        logger.debug(f"Enqueued job {job_name} [{job['jobId']}] using job definition {job_def_arn}.")

        return job['jobId']

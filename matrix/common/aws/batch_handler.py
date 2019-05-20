import os

import boto3
from tenacity import retry, stop_after_attempt, wait_fixed

from matrix.common.aws.dynamo_handler import DynamoTable
from matrix.common.aws.cloudwatch_handler import CloudwatchHandler, MetricName
from matrix.common.constants import MatrixFormat
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class BatchHandler:
    def __init__(self):
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']
        self.s3_results_bucket = os.environ.get('MATRIX_QUERY_RESULTS_BUCKET')
        self.job_queue_arn = os.environ.get('BATCH_CONVERTER_JOB_QUEUE_ARN')
        self.job_def_arn = os.environ.get('BATCH_CONVERTER_JOB_DEFINITION_ARN')
        self._cloudwatch_handler = CloudwatchHandler()
        self._client = boto3.client("batch", region_name=os.environ['AWS_DEFAULT_REGION'])

    @retry(reraise=True, wait=wait_fixed(2), stop=stop_after_attempt(5))
    def schedule_matrix_conversion(self, request_id: str, format: str):
        """
        Schedule a matrix conversion job within aws batch infra

        :param request_id: UUID identifying a matrix service request.
        :param format: User requested output file format of final expression matrix.
        """
        Logging.set_correlation_id(logger, value=request_id)
        job_name = "-".join(["conversion",
                             self.deployment_stage,
                             request_id,
                             format])

        is_compressed = format == MatrixFormat.CSV.value or format == MatrixFormat.MTX.value
        source_expression_manifest = f"s3://{self.s3_results_bucket}/{request_id}/expression_manifest"
        source_cell_manifest = f"s3://{self.s3_results_bucket}/{request_id}/cell_metadata_manifest"
        source_gene_manifest = f"s3://{self.s3_results_bucket}/{request_id}/gene_metadata_manifest"
        target_path = f"s3://{self.s3_results_bucket}/{request_id}.{format}" + (".zip" if is_compressed else "")
        working_dir = "/data"
        command = ['python3',
                   '/matrix_converter.py',
                   request_id,
                   source_expression_manifest,
                   source_cell_manifest,
                   source_gene_manifest,
                   target_path,
                   format,
                   working_dir]

        environment = {
            'DEPLOYMENT_STAGE': self.deployment_stage,
            'DYNAMO_REQUEST_TABLE_NAME': DynamoTable.REQUEST_TABLE.value,
        }

        batch_job_id = self._enqueue_batch_job(job_name=job_name,
                                               job_queue_arn=self.job_queue_arn,
                                               job_def_arn=self.job_def_arn,
                                               command=command,
                                               environment=environment)
        self._cloudwatch_handler.put_metric_data(
            metric_name=MetricName.CONVERSION_REQUEST,
            metric_value=1
        )
        return batch_job_id

    @retry(reraise=True, wait=wait_fixed(2), stop=stop_after_attempt(5))
    def get_batch_job_status(self, batch_job_id):
        response = self._client.describe_jobs(jobs=[batch_job_id])
        jobs = response.get("jobs")
        status = jobs[0]["status"]
        return status

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

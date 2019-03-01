import os

import boto3

from matrix.common.aws.dynamo_handler import DynamoTable
from matrix.common.constants import MatrixFormat
from matrix.common.logging import Logging
from matrix.common.aws.cloudwatch_handler import CloudwatchHandler, MetricName

logger = Logging.get_logger(__name__)


class BatchHandler:
    def __init__(self):
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']
        self.s3_results_bucket = os.environ['MATRIX_RESULTS_BUCKET']
        self.job_queue_arn = os.environ['BATCH_CONVERTER_JOB_QUEUE_ARN']
        self.job_def_arn = os.environ['BATCH_CONVERTER_JOB_DEFINITION_ARN']

        self._client = boto3.client("batch", region_name=os.environ['AWS_DEFAULT_REGION'])
        self._cloudwatch_handler = CloudwatchHandler()

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
        source_expression_manifest = f"s3://{self.s3_results_bucket}/{request_id}/expressionmanifest"
        source_cell_manifest = f"s3://{self.s3_results_bucket}/{request_id}/cell_metadatamanifest"
        source_gene_manifest = f"s3://{self.s3_results_bucket}/{request_id}/gene_metadatamanifest"
        target_path = f"s3://{self.s3_results_bucket}/{request_id}.{format}" + (".zip" if is_compressed else "")
        command = ['python3',
                   '/matrix_converter.py',
                   request_id,
                   source_expression_manifest,
                   source_cell_manifest,
                   source_gene_manifest,
                   target_path,
                   format]

        environment = {
            'DEPLOYMENT_STAGE': self.deployment_stage,
            'DYNAMO_STATE_TABLE_NAME': DynamoTable.STATE_TABLE.value,
            'DYNAMO_OUTPUT_TABLE_NAME': DynamoTable.OUTPUT_TABLE.value,
        }

        self._enqueue_batch_job(job_name=job_name,
                                job_queue_arn=self.job_queue_arn,
                                job_def_arn=self.job_def_arn,
                                command=command,
                                environment=environment)
        self._cloudwatch_handler.put_metric_data(
            metric_name=MetricName.CONVERSION_REQUEST,
            metric_value=1
        )

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

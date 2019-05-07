import typing
import os

from matrix.common import query_constructor
from matrix.common.config import MatrixInfraConfig, MatrixRedshiftConfig
from matrix.common.logging import Logging
from matrix.common.request.request_tracker import RequestTracker, Subtask
from matrix.common.aws.dynamo_handler import DynamoHandler
from matrix.common.aws.sqs_handler import SQSHandler
from matrix.common.aws.s3_handler import S3Handler

logger = Logging.get_logger(__name__)


class Driver:
    """
    Formats and stores redshift queries in s3 and sqs for execution.
    """
    def __init__(self, request_id: str):
        Logging.set_correlation_id(logger, value=request_id)

        self.request_id = request_id
        self.request_tracker = RequestTracker(request_id)
        self.dynamo_handler = DynamoHandler()
        self.sqs_handler = SQSHandler()
        self.infra_config = MatrixInfraConfig()
        self.redshift_config = MatrixRedshiftConfig()
        self.results_bucket = os.environ['MATRIX_RESULTS_BUCKET']
        self.s3_handler = S3Handler(os.environ['MATRIX_QUERY_BUCKET'])

    @property
    def query_job_q_url(self):
        return self.infra_config.query_job_q_url

    @property
    def redshift_role_arn(self):
        return self.redshift_config.redshift_role_arn

    def run(self, filter_: typing.Dict[str, typing.Any], fields: typing.List[str], feature: str):
        """
        Initialize a matrix service request and spawn redshift queries.

        :param filter_: Filter dict describing which cells to get expression data for
        :param fields: Which metadata fields to return
        :param format: MatrixFormat file format of output expression matrix
        :param feature: Which feature (gene vs transcript) to include in output
        """
        logger.debug(f"Driver running with parameters: filter={filter_}, "
                     f"fields={fields}, feature={feature}")

        matrix_request_queries = query_constructor.create_matrix_request_queries(
            filter_, fields, feature)

        s3_obj_keys = self._format_and_store_queries_in_s3(matrix_request_queries)
        for s3_obj_key in s3_obj_keys:
            self._add_request_query_to_sqs(s3_obj_key)
        self.request_tracker.complete_subtask_execution(Subtask.DRIVER)

    def _format_and_store_queries_in_s3(self, queries: list):
        feature_query = queries["feature_query"].format(results_bucket=self.results_bucket,
                                                        request_id=self.request_id,
                                                        iam_role=self.redshift_role_arn)
        feature_query_obj_key = self.s3_handler.store_content_in_s3(f"{self.request_id}/feature", feature_query)

        exp_query = queries["expression_query"].format(results_bucket=self.results_bucket,
                                                       request_id=self.request_id,
                                                       iam_role=self.redshift_role_arn)
        exp_query_obj_key = self.s3_handler.store_content_in_s3(f"{self.request_id}/expression", exp_query)

        cell_query = queries["cell_query"].format(requests_bucket=self.results_bucket,
                                                  request_id=self.request_id,
                                                  iam_role=self.redshift_role_arn)
        cell_query_obj_key = self.s3_handler.store_content_in_s3(f"{self.request_id}/cell", cell_query)

        return [feature_query_obj_key, exp_query_obj_key, cell_query_obj_key]

    def _add_request_query_to_sqs(self, s3_obj_key: str):
        queue_url = self.query_job_q_url
        payload = {
            'request_id': self.request_id,
            's3_obj_key': s3_obj_key
        }
        logger.debug(f"Adding {payload} to sqs {queue_url}")
        self.sqs_handler.add_message_to_queue(queue_url, payload)

import typing
import os

from matrix.common import query_constructor
from matrix.common.config import MatrixInfraConfig, MatrixRedshiftConfig
from matrix.common.logging import Logging
from matrix.common.request.request_tracker import RequestTracker, Subtask
from matrix.common.aws.dynamo_handler import DynamoHandler
from matrix.common.aws.sqs_handler import SQSHandler
from matrix.common.aws.s3_handler import S3Handler
from matrix.docker.query_runner import QueryType

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
        self.query_results_bucket = os.environ['MATRIX_QUERY_RESULTS_BUCKET']
        self.s3_handler = S3Handler(os.environ['MATRIX_QUERY_BUCKET'])

    @property
    def query_job_q_url(self):
        return self.infra_config.query_job_q_url

    @property
    def redshift_role_arn(self):
        return self.redshift_config.redshift_role_arn

    def run(self, filter_: typing.Dict[str, typing.Any], fields: typing.List[str], feature: str, genus_species: str):
        """
        Initialize a matrix service request and spawn redshift queries.

        :param filter_: Filter dict describing which cells to get expression data for
        :param fields: Which metadata fields to return
        :param format: MatrixFormat file format of output expression matrix
        :param feature: Which feature (gene vs transcript) to include in output
        """
        logger.debug(f"Driver running with parameters: filter={filter_}, "
                     f"fields={fields}, feature={feature}")

        try:
            matrix_request_queries = query_constructor.create_matrix_request_queries(
                query_constructor.speciesify_filter(filter_, genus_species),
                fields,
                feature)
        except (query_constructor.MalformedMatrixFilter, query_constructor.MalformedMatrixFeature) as exc:
            self.request_tracker.log_error(f"Query construction failed with error: {str(exc)}")
            raise

        s3_obj_keys = self._format_and_store_queries_in_s3(matrix_request_queries, genus_species)
        for key in s3_obj_keys:
            self._add_request_query_to_sqs(key, s3_obj_keys[key])

        self.request_tracker.complete_subtask_execution(Subtask.DRIVER)

    def _format_and_store_queries_in_s3(self, queries: dict, genus_species: str):
        feature_query = queries[QueryType.FEATURE].format(results_bucket=self.query_results_bucket,
                                                          request_id=self.request_id,
                                                          genus_species=genus_species,
                                                          iam_role=self.redshift_role_arn)
        feature_query_obj_key = self.s3_handler.store_content_in_s3(
            f"{self.request_id}/{QueryType.FEATURE.value}",
            feature_query)

        exp_query = queries[QueryType.EXPRESSION].format(results_bucket=self.query_results_bucket,
                                                         request_id=self.request_id,
                                                         genus_species=genus_species,
                                                         iam_role=self.redshift_role_arn)
        exp_query_obj_key = self.s3_handler.store_content_in_s3(
            f"{self.request_id}/{QueryType.EXPRESSION.value}",
            exp_query)

        cell_query = queries[QueryType.CELL].format(results_bucket=self.query_results_bucket,
                                                    request_id=self.request_id,
                                                    genus_species=genus_species,
                                                    iam_role=self.redshift_role_arn)
        cell_query_obj_key = self.s3_handler.store_content_in_s3(
            f"{self.request_id}/{QueryType.CELL.value}",
            cell_query)

        return {
            QueryType.CELL: cell_query_obj_key,
            QueryType.EXPRESSION: exp_query_obj_key,
            QueryType.FEATURE: feature_query_obj_key
        }

    def _add_request_query_to_sqs(self, query_type: QueryType, s3_obj_key: str):
        queue_url = self.query_job_q_url
        payload = {
            'request_id': self.request_id,
            's3_obj_key': s3_obj_key,
            'type': query_type.value
        }
        logger.debug(f"Adding {payload} to sqs {queue_url}")
        self.sqs_handler.add_message_to_queue(queue_url, payload)

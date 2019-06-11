import typing
import os

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from matrix.common.config import MatrixInfraConfig, MatrixRedshiftConfig
from matrix.common.logging import Logging
from matrix.common.request.request_tracker import RequestTracker, Subtask
from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, RequestTableField
from matrix.common.aws.redshift_handler import RedshiftHandler
from matrix.common.aws.sqs_handler import SQSHandler
from matrix.common.aws.s3_handler import S3Handler

logger = Logging.get_logger(__name__)

analysis_bundle_count_query_template = """
    SELECT count(*)
    FROM analysis
    WHERE bundle_fqid IN {0};
"""

expression_query_template = """
    UNLOAD ($$SELECT cell.cellkey, expression.featurekey, expression.exrpvalue
    FROM expression
    LEFT OUTER JOIN feature on (expression.featurekey = feature.featurekey)
    INNER JOIN cell on (expression.cellkey = cell.cellkey)
    INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
    WHERE feature.isgene
    AND expression.exprtype = 'Count'
    AND analysis.bundle_fqid IN {3}$$)
    TO 's3://{0}/{1}/expression_'
    IAM_ROLE '{2}'
    GZIP
    MANIFEST VERBOSE
    ;
"""

cell_query_template = """
    UNLOAD($$SELECT cell.cellkey, cell.cell_suspension_id, cell.genes_detected, specimen.*, library_preparation.*,
    project.short_name
    FROM cell
    LEFT OUTER JOIN specimen on (cell.specimenkey = specimen.specimenkey)
    LEFT OUTER JOIN library_preparation on (cell.librarykey = library_preparation.librarykey)
    LEFT OUTER JOIN project on (cell.projectkey = project.projectkey)
    INNER JOIN analysis on (cell.analysiskey = analysis.analysiskey)
    WHERE analysis.bundle_fqid IN {3}$$)
    TO 's3://{0}/{1}/cell_metadata_'
    IAM_ROLE '{2}'
    GZIP
    MANIFEST VERBOSE
    ;
"""

feature_query_template = """
    UNLOAD ($$SELECT *
    FROM feature
    WHERE feature.isgene$$)
    to 's3://{0}/{1}/gene_metadata_'
    IAM_ROLE '{2}'
    GZIP
    MANIFEST VERBOSE;
"""


class Driver:
    """
    Formats and stores redshift queries in s3 and sqs for execution.
    """
    def __init__(self, request_id: str, bundles_per_worker: int=100):
        Logging.set_correlation_id(logger, value=request_id)

        self.request_id = request_id
        self.bundles_per_worker = bundles_per_worker
        self.request_tracker = RequestTracker(request_id)
        self.dynamo_handler = DynamoHandler()
        self.sqs_handler = SQSHandler()
        self.infra_config = MatrixInfraConfig()
        self.redshift_config = MatrixRedshiftConfig()
        self.query_results_bucket = os.environ['MATRIX_QUERY_RESULTS_BUCKET']
        self.s3_handler = S3Handler(os.environ['MATRIX_QUERY_BUCKET'])
        self.redshift_handler = RedshiftHandler()

    @property
    def query_job_q_url(self):
        return self.infra_config.query_job_q_url

    @property
    def redshift_role_arn(self):
        return self.redshift_config.redshift_role_arn

    def run(self, bundle_fqids: typing.List[str], bundle_fqids_url: str, format: str):
        """
        Initialize a matrix service request and spawn redshift queries.

        :param bundle_fqids: List of bundle fqids to be queried on
        :param bundle_fqids_url: URL from which bundle_fqids can be retrieved
        :param format: MatrixFormat file format of output expression matrix
        """
        logger.debug(f"Driver running with parameters: bundle_fqids={bundle_fqids}, "
                     f"bundle_fqids_url={bundle_fqids_url}, format={format}, "
                     f"bundles_per_worker={self.bundles_per_worker}")

        if bundle_fqids_url:
            response = self._get_bundle_manifest(bundle_fqids_url)
            resolved_bundle_fqids = self._parse_download_manifest(response.text)
        else:
            resolved_bundle_fqids = bundle_fqids
        logger.debug(f"resolved bundles: {resolved_bundle_fqids}")

        self.dynamo_handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE,
                                                       self.request_id,
                                                       RequestTableField.NUM_BUNDLES,
                                                       len(resolved_bundle_fqids))
        s3_obj_keys = self._format_and_store_queries_in_s3(resolved_bundle_fqids)

        analysis_table_bundle_count = self._fetch_bundle_count_from_analysis_table(resolved_bundle_fqids)
        if analysis_table_bundle_count != len(resolved_bundle_fqids):
            error_msg = "resolved bundles in request do not match bundles available in matrix service"
            logger.info(error_msg)
            self.request_tracker.log_error(error_msg)
            return

        for s3_obj_key in s3_obj_keys:
            self._add_request_query_to_sqs(s3_obj_key)
        self.request_tracker.complete_subtask_execution(Subtask.DRIVER)

    @retry(reraise=True, wait=wait_fixed(5), stop=stop_after_attempt(60))
    def _get_bundle_manifest(self, bundle_fqids_url):
        response = requests.get(bundle_fqids_url)
        return response

    @staticmethod
    def _parse_download_manifest(data: str) -> typing.List[str]:
        def _parse_line(line: str) -> str:
            tokens = line.split("\t")
            return f"{tokens[0]}.{tokens[1]}"

        lines = data.splitlines()[1:]
        return list(map(_parse_line, lines))

    @staticmethod
    def _format_bundle_fqids(bundle_fqids: typing.List[str]) -> str:
        return '(' + ', '.join("'" + str(b) + "'" for b in bundle_fqids) + ')'

    def _format_and_store_queries_in_s3(self, resolved_bundle_fqids: list):
        feature_query = feature_query_template.format(self.query_results_bucket,
                                                      self.request_id,
                                                      self.redshift_role_arn)
        feature_query_obj_key = self.s3_handler.store_content_in_s3(f"{self.request_id}/feature", feature_query)

        exp_query = expression_query_template.format(self.query_results_bucket,
                                                     self.request_id,
                                                     self.redshift_role_arn,
                                                     self._format_bundle_fqids(resolved_bundle_fqids))
        exp_query_obj_key = self.s3_handler.store_content_in_s3(f"{self.request_id}/expression", exp_query)

        cell_query = cell_query_template.format(self.query_results_bucket,
                                                self.request_id,
                                                self.redshift_role_arn,
                                                self._format_bundle_fqids(resolved_bundle_fqids))
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

    def _fetch_bundle_count_from_analysis_table(self, resolved_bundle_fqids: list):
        analysis_table_bundle_count_query = analysis_bundle_count_query_template.format(
            self._format_bundle_fqids(resolved_bundle_fqids))
        analysis_table_bundle_count_query = analysis_table_bundle_count_query.strip().replace('\n', '')
        results = self.redshift_handler.transaction([analysis_table_bundle_count_query],
                                                    read_only=True,
                                                    return_results=True)
        analysis_table_bundle_count = results[0][0]
        return analysis_table_bundle_count

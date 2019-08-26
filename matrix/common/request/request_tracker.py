import hashlib
import os
from datetime import timedelta
from enum import Enum

from matrix.common import date
from matrix.common.constants import DEFAULT_FIELDS, DEFAULT_FEATURE
from matrix.common.aws.batch_handler import BatchHandler
from matrix.common.aws.cloudwatch_handler import CloudwatchHandler, MetricName
from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, RequestTableField
from matrix.common.aws.s3_handler import S3Handler
from matrix.common.exceptions import MatrixException
from matrix.common.logging import Logging
from matrix.common.query.cell_query_results_reader import CellQueryResultsReader
from matrix.common.query.query_results_reader import MatrixQueryResultsNotFound
from matrix.common.constants import MatrixFormat

logger = Logging.get_logger(__name__)


class Subtask(Enum):
    """
    A Subtask represents a processing stage in a matrix service request.

    Driver - daemons/driver.py
    Converter - common/matrix_converter.py
    """
    DRIVER = "driver"
    CONVERTER = "converter"
    QUERY = "query"


class RequestTracker:
    """
    Provides an interface for tracking a request's parameters and state.
    """

    def __init__(self, request_id: str):
        Logging.set_correlation_id(logger, request_id)

        self.request_id = request_id
        self._request_hash = "N/A"
        self._data_version = None
        self._num_bundles = None
        self._format = None
        self._metadata_fields = None
        self._feature = None

        self.dynamo_handler = DynamoHandler()
        self.cloudwatch_handler = CloudwatchHandler()
        self.batch_handler = BatchHandler()

    @property
    def is_initialized(self) -> bool:
        try:
            self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE,
                                               request_id=self.request_id)
        except MatrixException:
            return False

        return True

    @property
    def request_hash(self) -> str:
        """
        Unique hash generated using request parameters.
        If a request hash does not exist, one will be attempted to be generated.
        :return: str Request hash
        """
        if self._request_hash == "N/A":
            self._request_hash = self.dynamo_handler.get_table_item(
                DynamoTable.REQUEST_TABLE,
                request_id=self.request_id
            )[RequestTableField.REQUEST_HASH.value]

            # Do not generate request hash in API requests to avoid timeouts.
            # Presence of MATRIX_VERSION indicates API deployment.
            if self._request_hash == "N/A" and not os.getenv('MATRIX_VERSION'):
                try:
                    self._request_hash = self.generate_request_hash()
                    self.dynamo_handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE,
                                                                   self.request_id,
                                                                   RequestTableField.REQUEST_HASH,
                                                                   self._request_hash)
                except MatrixQueryResultsNotFound as e:
                    logger.warning(f"Failed to generate a request hash. {e}")

        return self._request_hash

    @property
    def s3_results_prefix(self) -> str:
        """
        The S3 prefix where results for this request hash are stored in the results bucket.
        :return: str S3 prefix
        """
        return f"{self.data_version}/{self.request_hash}"

    @property
    def s3_results_key(self) -> str:
        """
        The S3 key where matrix results for this request are stored in the results bucket.
        :return: str S3 key
        """
        is_compressed = self.format == MatrixFormat.CSV.value or self.format == MatrixFormat.MTX.value

        return f"{self.data_version}/{self.request_hash}/{self.request_id}.{self.format}" + \
               (".zip" if is_compressed else "")

    @property
    def data_version(self) -> int:
        """
        The Redshift data version this request is generated on.
        :return: int Data version
        """
        if self._data_version is None:
            self._data_version = \
                self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE,
                                                   request_id=self.request_id)[RequestTableField.DATA_VERSION.value]

        return self._data_version

    @property
    def num_bundles(self) -> int:
        """
        The number of bundles in the request.
        :return: int Number of bundles
        """
        if not self._num_bundles:
            self._num_bundles = \
                self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE,
                                                   request_id=self.request_id)[RequestTableField.NUM_BUNDLES.value]
        return self._num_bundles

    @property
    def num_bundles_interval(self) -> str:
        """
        Returns the interval string that num_bundles corresponds to.
        :return: the interval string e.g. "0-499"
        """
        interval_size = 500

        index = int(self.num_bundles / interval_size)
        return f"{index * interval_size}-{(index * interval_size) + interval_size - 1}"

    @property
    def format(self) -> str:
        """
        The request's user specified output file format of the resultant expression matrix.
        :return: str The file format (one of MatrixFormat)
        """
        if not self._format:
            self._format = \
                self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE,
                                                   request_id=self.request_id)[RequestTableField.FORMAT.value]
        return self._format

    @property
    def metadata_fields(self) -> list:
        """
        The request's user-specified list of metadata fields to include in the resultant expression matrix.
        :return:  list List of metadata fields
        """
        if not self._metadata_fields:
            self._metadata_fields = \
                self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE,
                                                   request_id=self.request_id)[RequestTableField.METADATA_FIELDS.value]
        return self._metadata_fields

    @property
    def feature(self) -> str:
        """
        The request's user-specified feature type (gene|transcript) of the resultant expression matrix.
        :return: str Feature (gene|transcript)
        """
        if not self._feature:
            self._feature = \
                self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE,
                                                   request_id=self.request_id)[RequestTableField.FEATURE.value]
        return self._feature

    @property
    def batch_job_id(self) -> str:
        """
        The batch job id for matrix conversion corresponding with a request.
        :return: str The batch job id
        """
        table_item = self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE, request_id=self.request_id)
        batch_job_id = table_item.get(RequestTableField.BATCH_JOB_ID.value)
        if not batch_job_id or batch_job_id == "N/A":
            return None
        else:
            return batch_job_id

    @property
    def batch_job_status(self) -> str:
        """
        The batch job status for matrix conversion corresponding with a request.
        :return: str The batch job status
        """
        status = None
        if self.batch_job_id:
            status = self.batch_handler.get_batch_job_status(self.batch_job_id)
        return status

    @property
    def creation_date(self) -> str:
        """
        The creation date of matrix service request.
        :return: str creation date
        """
        return self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE,
                                                  request_id=self.request_id)[RequestTableField.CREATION_DATE.value]

    @property
    def is_expired(self):
        """
        Whether or not the request has expired and the matrix in S3 has been deleted.
        :return: bool
        """
        s3_results_bucket_handler = S3Handler(os.environ['MATRIX_RESULTS_BUCKET'])
        is_past_expiration = date.to_datetime(self.creation_date) < date.get_datetime_now() - timedelta(days=30)
        is_expired = not s3_results_bucket_handler.exists(self.s3_results_key) and is_past_expiration

        if is_expired:
            self.log_error("This request has expired after 30 days and is no longer available for download. "
                           "A new matrix can be generated by resubmitting the POST request to /v1/matrix.")

        return is_expired

    @property
    def timeout(self) -> bool:
        timeout = date.to_datetime(self.creation_date) < date.get_datetime_now() - timedelta(hours=36)
        if timeout:
            self.log_error("This request has timed out after 12 hours."
                           "Please try again by resubmitting the POST request.")
        return timeout

    @property
    def error(self) -> str:
        """
        The user-friendly message describing the latest error the request raised.
        :return: str The error message if one exists, else empty string
        """
        error = self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE,
                                                   request_id=self.request_id)[RequestTableField.ERROR_MESSAGE.value]
        return error if error else ""

    def initialize_request(self,
                           fmt: str,
                           metadata_fields: list = DEFAULT_FIELDS,
                           feature: str = DEFAULT_FEATURE) -> None:
        """Initialize the request id in the request state table. Put request metric to cloudwatch.
        :param fmt: Request output format for matrix conversion
        :param metadata_fields: Metadata fields to include in expression matrix
        :param feature: Feature type to generate expression counts for (one of MatrixFeature)
        """
        self.dynamo_handler.create_request_table_entry(self.request_id,
                                                       fmt,
                                                       metadata_fields,
                                                       feature)
        self.cloudwatch_handler.put_metric_data(
            metric_name=MetricName.REQUEST,
            metric_value=1
        )

    def generate_request_hash(self) -> str:
        """
        Generates a request hash uniquely identifying a request by its input parameters.
        Requires cell query results to exist, else raises MatrixQueryResultsNotFound.
        :return: str Request hash
        """
        cell_manifest_key = f"s3://{os.environ['MATRIX_QUERY_RESULTS_BUCKET']}/{self.request_id}/cell_metadata_manifest"
        reader = CellQueryResultsReader(cell_manifest_key)
        cell_df = reader.load_results()
        cellkeys = cell_df.index

        h = hashlib.md5()
        h.update(self.feature.encode())
        h.update(self.format.encode())

        for field in self.metadata_fields:
            h.update(field.encode())

        for key in cellkeys:
            h.update(key.encode())

        request_hash = h.hexdigest()

        return request_hash

    def expect_subtask_execution(self, subtask: Subtask):
        """
        Expect the execution of 1 Subtask by tracking it in DynamoDB.
        A Subtask is executed either by a Lambda or AWS Batch.
        :param subtask: The expected Subtask to be executed.
        """
        subtask_to_dynamo_field_name = {
            Subtask.DRIVER: RequestTableField.EXPECTED_DRIVER_EXECUTIONS,
            Subtask.CONVERTER: RequestTableField.EXPECTED_CONVERTER_EXECUTIONS,
        }

        self.dynamo_handler.increment_table_field(DynamoTable.REQUEST_TABLE,
                                                  self.request_id,
                                                  subtask_to_dynamo_field_name[subtask],
                                                  1)

    def complete_subtask_execution(self, subtask: Subtask):
        """
        Counts the completed execution of 1 Subtask in DynamoDB.
        A Subtask is executed either by a Lambda or AWS Batch.
        :param subtask: The executed Subtask.
        """
        subtask_to_dynamo_field_name = {
            Subtask.DRIVER: RequestTableField.COMPLETED_DRIVER_EXECUTIONS,
            Subtask.QUERY: RequestTableField.COMPLETED_QUERY_EXECUTIONS,
            Subtask.CONVERTER: RequestTableField.COMPLETED_CONVERTER_EXECUTIONS,
        }

        self.dynamo_handler.increment_table_field(DynamoTable.REQUEST_TABLE,
                                                  self.request_id,
                                                  subtask_to_dynamo_field_name[subtask],
                                                  1)

    def lookup_cached_result(self) -> str:
        """
        Retrieves the S3 key of an existing matrix result that corresponds to this request's request hash.
        Returns "" if no such result exists
        :return: S3 key of cached result
        """
        results_bucket = S3Handler(os.environ['MATRIX_RESULTS_BUCKET'])
        objects = results_bucket.ls(f"{self.s3_results_prefix}/")

        if len(objects) > 0:
            return objects[0]['Key']
        return ""

    def is_request_ready_for_conversion(self) -> bool:
        """
        Checks whether the request has completed all queries
        and is ready for conversion
        :return: bool True if complete, else False
        """
        request_state = self.dynamo_handler.get_table_item(DynamoTable.REQUEST_TABLE, request_id=self.request_id)
        queries_complete = (request_state[RequestTableField.EXPECTED_QUERY_EXECUTIONS.value]
                            == request_state[RequestTableField.COMPLETED_QUERY_EXECUTIONS.value])
        return queries_complete

    def is_request_complete(self) -> bool:
        """
        Checks whether the request has completed.
        :return: bool True if complete, else False
        """
        results_bucket = S3Handler(os.environ['MATRIX_RESULTS_BUCKET'])
        return results_bucket.exists(self.s3_results_key)

    def complete_request(self, duration: float):
        """
        Log the completion of a matrix request in CloudWatch Metrics
        :param duration: The time in seconds the request took to complete
        """
        self.cloudwatch_handler.put_metric_data(
            metric_name=MetricName.CONVERSION_COMPLETION,
            metric_value=1
        )
        self.cloudwatch_handler.put_metric_data(
            metric_name=MetricName.REQUEST_COMPLETION,
            metric_value=1
        )
        self.cloudwatch_handler.put_metric_data(
            metric_name=MetricName.DURATION,
            metric_value=duration,
            metric_dimensions=[
                {
                    'Name': "Number of Bundles",
                    'Value': self.num_bundles_interval
                },
                {
                    'Name': "Output Format",
                    'Value': self.format
                },
            ]
        )
        self.cloudwatch_handler.put_metric_data(
            metric_name=MetricName.DURATION,
            metric_value=duration,
            metric_dimensions=[
                {
                    'Name': "Number of Bundles",
                    'Value': self.num_bundles_interval
                },
            ]
        )

    def log_error(self, message: str):
        """
        Logs the latest error this request reported overwriting the previously logged error.
        :param message: str The error message to log
        """
        logger.debug(message)
        self.dynamo_handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE,
                                                       self.request_id,
                                                       RequestTableField.ERROR_MESSAGE,
                                                       message)
        self.cloudwatch_handler.put_metric_data(
            metric_name=MetricName.REQUEST_ERROR,
            metric_value=1
        )

    def write_batch_job_id_to_db(self, batch_job_id: str):
        """
        Logs the batch job id for matrix conversion to state table
        """
        self.dynamo_handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE,
                                                       self.request_id,
                                                       RequestTableField.BATCH_JOB_ID,
                                                       batch_job_id)

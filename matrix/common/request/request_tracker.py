from datetime import timedelta
from enum import Enum

from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, StateTableField, OutputTableField
from matrix.common.aws.batch_handler import BatchHandler
from matrix.common import date
from matrix.common.exceptions import MatrixException
from matrix.common.logging import Logging
from matrix.common.aws.cloudwatch_handler import CloudwatchHandler, MetricName

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
        self._num_bundles = None
        self._format = None

        self.dynamo_handler = DynamoHandler()
        self.cloudwatch_handler = CloudwatchHandler()
        self.batch_handler = BatchHandler()

    @property
    def is_initialized(self) -> bool:
        try:
            self.dynamo_handler.get_table_item(DynamoTable.STATE_TABLE,
                                               request_id=self.request_id)
        except MatrixException:
            return False

        return True

    @property
    def num_bundles(self) -> int:
        """
        The number of bundles in the request.
        :return: int Number of bundles
        """
        if not self._num_bundles:
            self._num_bundles =\
                self.dynamo_handler.get_table_item(DynamoTable.OUTPUT_TABLE,
                                                   request_id=self.request_id)[OutputTableField.NUM_BUNDLES.value]
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
            self._format =\
                self.dynamo_handler.get_table_item(DynamoTable.OUTPUT_TABLE,
                                                   request_id=self.request_id)[OutputTableField.FORMAT.value]
        return self._format

    @property
    def batch_job_id(self) -> str:
        """
        The batch job id for matrix conversion corresponding with a request.
        :return: str The batch job id
        """
        table_item = self.dynamo_handler.get_table_item(DynamoTable.STATE_TABLE, request_id=self.request_id)
        batch_job_id = table_item.get(StateTableField.BATCH_JOB_ID.value)
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
        return self.dynamo_handler.get_table_item(DynamoTable.STATE_TABLE,
                                                  request_id=self.request_id)[StateTableField.CREATION_DATE.value]

    @property
    def timeout(self) -> bool:
        timeout = date.to_datetime(self.creation_date) < date.get_datetime_now() - timedelta(hours=12)
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
        error = self.dynamo_handler.get_table_item(DynamoTable.OUTPUT_TABLE,
                                                   request_id=self.request_id)[OutputTableField.ERROR_MESSAGE.value]
        return error if error else ""

    def initialize_request(self, format: str) -> None:
        """Initialize the request id in the request state table. Put request metric to cloudwatch.
        """
        self.dynamo_handler.create_state_table_entry(self.request_id)
        self.cloudwatch_handler.put_metric_data(
            metric_name=MetricName.REQUEST,
            metric_value=1
        )

    def expect_subtask_execution(self, subtask: Subtask):
        """
        Expect the execution of 1 Subtask by tracking it in DynamoDB.
        A Subtask is executed either by a Lambda or AWS Batch.
        :param subtask: The expected Subtask to be executed.
        """
        subtask_to_dynamo_field_name = {
            Subtask.DRIVER: StateTableField.EXPECTED_DRIVER_EXECUTIONS,
            Subtask.CONVERTER: StateTableField.EXPECTED_CONVERTER_EXECUTIONS,
        }

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
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
            Subtask.DRIVER: StateTableField.COMPLETED_DRIVER_EXECUTIONS,
            Subtask.QUERY: StateTableField.COMPLETED_QUERY_EXECUTIONS,
            Subtask.CONVERTER: StateTableField.COMPLETED_CONVERTER_EXECUTIONS,
        }

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  subtask_to_dynamo_field_name[subtask],
                                                  1)

    def is_request_complete(self) -> bool:
        """
        Checks whether the request has completed,
        i.e. if all expected reducers and converters have completed.
        :return: bool True if complete, else False
        """
        request_state = self.dynamo_handler.get_table_item(DynamoTable.STATE_TABLE, request_id=self.request_id)
        queries_complete = (request_state[StateTableField.EXPECTED_QUERY_EXECUTIONS.value] ==
                            request_state[StateTableField.COMPLETED_QUERY_EXECUTIONS.value])
        converter_complete = (request_state[StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value] ==
                              request_state[StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value])

        return queries_complete and converter_complete

    def is_request_ready_for_conversion(self) -> bool:
        """
        Checks whether the request has completed all queries
        and is ready for conversion
        :return: bool True if complete, else False
        """
        request_state = self.dynamo_handler.get_table_item(DynamoTable.STATE_TABLE, request_id=self.request_id)
        queries_complete = (request_state[StateTableField.EXPECTED_QUERY_EXECUTIONS.value] ==
                            request_state[StateTableField.COMPLETED_QUERY_EXECUTIONS.value])
        return queries_complete

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
        self.dynamo_handler.set_table_field_with_value(DynamoTable.OUTPUT_TABLE,
                                                       self.request_id,
                                                       OutputTableField.ERROR_MESSAGE,
                                                       message)
        self.cloudwatch_handler.put_metric_data(
            metric_name=MetricName.REQUEST_ERROR,
            metric_value=1
        )

    def write_batch_job_id_to_db(self, batch_job_id: str):
        """
        Logs the batch job id for matrix conversion to state table
        """
        self.dynamo_handler.set_table_field_with_value(DynamoTable.STATE_TABLE,
                                                       self.request_id,
                                                       StateTableField.BATCH_JOB_ID,
                                                       batch_job_id)

from enum import Enum

from matrix.common.dynamo_handler import DynamoHandler, DynamoTable, StateTableField, OutputTableField


class Subtask(Enum):
    """
    Maps request subtasks to corresponding state tracking field in DynamoDB.
    """
    DRIVER = StateTableField.COMPLETED_DRIVER_EXECUTIONS
    MAPPER = StateTableField.COMPLETED_MAPPER_EXECUTIONS
    WORKER = StateTableField.COMPLETED_WORKER_EXECUTIONS
    REDUCER = StateTableField.COMPLETED_REDUCER_EXECUTIONS
    CONVERTER = StateTableField.COMPLETED_CONVERTER_EXECUTIONS


class RequestTracker:
    """
    Provides an interface for tracking a request's parameters and state.
    """
    def __init__(self, request_id: str):
        self.request_id = request_id
        self._format = None

        self.dynamo_handler = DynamoHandler()

    @property
    def format(self) -> str:
        """
        The request's user specified output file format of the resultant expression matrix.
        :return: str The file format (one of MatrixFormat)
        """
        if not self._format:
            self._format = self.dynamo_handler.get_table_item(DynamoTable.OUTPUT_TABLE,
                                                              self.request_id)[OutputTableField.FORMAT.value]
        return self._format

    @property
    def error(self) -> str:
        """
        The user-friendly message describing the latest error the request raised.
        :return: str The error message if one exists, else empty string
        """
        error = self.dynamo_handler.get_table_item(DynamoTable.OUTPUT_TABLE,
                                                   self.request_id)[OutputTableField.ERROR.value]

        if error:
            return error
        return ""

    def init_request(self, num_mappers: int, format: str):
        """
        Creates a new request in the relevant DynamoDB tables for state and output tracking.
        :param num_mappers: Number of expected mapper nodes this request will run.
                            Currently, this is equal to the expected number of worker nodes as well.
        :param format: The request's user specified output file format of the resultant expression matrix.
        """
        num_workers = num_mappers
        self.dynamo_handler.create_state_table_entry(self.request_id, num_mappers, num_workers, format)
        self.dynamo_handler.create_output_table_entry(self.request_id, format)

    def complete_subtask_node(self, subtask: Subtask):
        """
        Tracks the completion exeuction of a subtask node.
        A Subtask is executed either by a Lambda or AWS Batch.
        :param subtask: The completed Subtaskcompleted.
        """
        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  subtask.value,
                                                  1)

    def is_reducer_ready(self) -> bool:
        """
        Checks whether the reducer of this request is ready to be invoked,
        i.e. if all expected mappers and workers have completed.
        :return: bool True if ready, else False
        """
        request_state = self.dynamo_handler.get_table_item(DynamoTable.STATE_TABLE, self.request_id)

        mappers_complete = (request_state[StateTableField.EXPECTED_MAPPER_EXECUTIONS.value] ==
                            request_state[StateTableField.COMPLETED_MAPPER_EXECUTIONS.value])
        workers_complete = (request_state[StateTableField.EXPECTED_WORKER_EXECUTIONS.value] ==
                            request_state[StateTableField.COMPLETED_WORKER_EXECUTIONS.value])

        return mappers_complete and workers_complete

    def is_request_complete(self) -> bool:
        """
        Checks whether the request has completed,
        i.e. if all expected reducers and converters have completed.
        :return: bool True if complete, else False
        """
        request_state = self.dynamo_handler.get_table_item(DynamoTable.STATE_TABLE, self.request_id)

        reducer_complete = (request_state[StateTableField.EXPECTED_REDUCER_EXECUTIONS.value] ==
                            request_state[StateTableField.COMPLETED_REDUCER_EXECUTIONS.value])
        converter_complete = (request_state[StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value] ==
                              request_state[StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value])

        return reducer_complete and converter_complete

    def write_error(self, message: str):
        self.dynamo_handler.write_request_error(self.request_id, message)

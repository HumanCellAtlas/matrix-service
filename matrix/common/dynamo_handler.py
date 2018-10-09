from enum import Enum
import os

import boto3


class StateTableField(Enum):
    """
    Field names for State table in DynamoDB.
    """
    REQUEST_ID = "RequestId"
    EXPECTED_MAPPER_EXECUTIONS = "ExpectedMapperExecutions"
    COMPLETED_MAPPER_EXECUTIONS = "CompletedMapperExecutions"
    EXPECTED_WORKER_EXECUTIONS = "ExpectedWorkerExecutions"
    COMPLETED_WORKER_EXECUTIONS = "CompletedWorkerExecutions"
    EXPECTED_REDUCER_EXECUTIONS = "ExpectedReducerExecutions"
    COMPLETED_REDUCER_EXECUTIONS = "CompletedReducerExecutions"


class DynamoHandler:
    """
    Interface for interacting with DynamoDB Tables.
    """
    def __init__(self):
        self._dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self._state_table = self._dynamo.Table(os.environ['DYNAMO_STATE_TABLE_NAME'])

    def init_state_table(self, request_id, num_bundles):
        """
        Initialize the DynamoDB table responsible for tracking task execution states and
        counts for a specified job.

        :param request_id: UUID identifying a filter merge job request.
        :param num_bundles: Number of bundles to be processed.
        """
        self._state_table.put_item(
            Item={
                StateTableField.REQUEST_ID.value: request_id,
                StateTableField.EXPECTED_WORKER_EXECUTIONS.value: 0,
                StateTableField.COMPLETED_WORKER_EXECUTIONS.value: 0,
                StateTableField.EXPECTED_MAPPER_EXECUTIONS.value: num_bundles,
                StateTableField.COMPLETED_MAPPER_EXECUTIONS.value: 0,
                StateTableField.EXPECTED_REDUCER_EXECUTIONS.value: 1,
                StateTableField.COMPLETED_REDUCER_EXECUTIONS.value: 0
            }
        )

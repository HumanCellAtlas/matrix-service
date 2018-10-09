import os
import time
from enum import Enum

import boto3
import botocore


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


class OutputTableField(Enum):
    """
    Field names for Output table in DynamoDB.
    """
    REQUEST_ID = "RequestId"
    ROW_COUNT = "RowCount"


class DynamoHandler:
    """
    Interface for interacting with DynamoDB Tables.
    """
    def __init__(self):
        self._dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self._state_table = self._dynamo.Table(os.getenv("DYNAMO_STATE_TABLE_NAME"))
        self._output_table = self._dynamo.Table(os.getenv("DYNAMO_OUTPUT_TABLE_NAME"))

    def create_state_table_entry(self, request_id: str, num_bundles: int):
        """
        Put a new item in the DynamoDB table responsible for tracking task execution states and
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
                StateTableField.COMPLETED_REDUCER_EXECUTIONS.value: 0,
            }
        )

    def create_output_table_entry(self, request_id: str):
        """
        Put a new item in the DynamoDB Table responsible for counting output rows
        :param request_id: UUID identifying a filter merge job request.
        """
        self._output_table.put_item(
            Item={
                OutputTableField.REQUEST_ID.value: request_id,
                OutputTableField.ROW_COUNT.value: 0,
            }
        )

    def increment_state_table_field(self, request_id: str, field_name: str, increment_size: int):
        """Increment value in state table
        Args:
            request_id: request id key in state table
            field_name: Name of the field to increment
            increment_size: Amount by which to increment the field.
        Returns:
            start_value, end_value: The values before and after incrementing
        """
        self._increment_field(self._state_table, {"RequestId": request_id}, field_name, increment_size)

    def increment_output_table_field(self, request_id: str, field_name: str, increment_size: int):
        """Increment value in output table
        Args:
            request_id: request id key in output table
            field_name: Name of the field to increment
            increment_size: Amount by which to increment the field.
        Returns:
            start_value, end_value: The values before and after incrementing
        """
        self._increment_field(self._output_table, {"RequestId": request_id}, field_name, increment_size)

    def _increment_field(self, table, key_dict: dict, field_name: str, increment_size: int):
        """Increment a value in a dynamo table safely.
        Makes sure distributed table updates don't clobber each other. For example,
        increment_field(dynamo_table_obj, {"id": id_}, "Counts", 5)
        will increment the Counts value in the item keyed by {"id": id_} in table
        "my_table" by 5.
        Args:
          table: boto3 resource for a dynamodb table
          key_dict: Dict for the key in the table
          field_name: Name of the field to increment
          increment_size: Amount by which to increment the field.
        Returns:
          start_value, end_value: The values before and after incrementing
        """

        while True:
            db_response = table.get_item(
                Key=key_dict,
                ConsistentRead=True
            )
            item = db_response["Item"]
            start_value = item[field_name]
            new_value = start_value + increment_size

            try:
                table.update_item(
                    Key=key_dict,
                    UpdateExpression=f"SET {field_name} = :n",
                    ConditionExpression=f"{field_name} = :s",
                    ExpressionAttributeValues={":n": new_value, ":s": start_value}
                )
                break
            except botocore.exceptions.ClientError as exc:
                if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    pass
                else:
                    raise
            time.sleep(.5)

        return start_value, new_value

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


class DynamoTable(Enum):
    """
    Names of dynamo tables in matrix service
    """
    STATE_TABLE = os.getenv("DYNAMO_STATE_TABLE_NAME")
    OUTPUT_TABLE = os.getenv("DYNAMO_OUTPUT_TABLE_NAME")


class DynamoHandler:
    """
    Interface for interacting with DynamoDB Tables.
    """
    def __init__(self):
        self._dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self._state_table = self._dynamo.Table(DynamoTable.STATE_TABLE.value)
        self._output_table = self._dynamo.Table(DynamoTable.OUTPUT_TABLE.value)

    def _get_dynamo_table_resource_from_enum(self, dynamo_table: DynamoTable):
        """Retrieve dynamo table resource for a given dynamo table name.

        Input:
            dynamo_table: (DynamoTable), Enum
        Output:
            boto3 dynamodb resource
        """
        name = dynamo_table.name
        if name == "STATE_TABLE":
            return self._state_table
        elif name == "OUTPUT_TABLE":
            return self._output_table

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

    def get_table_item(self, table: DynamoTable, request_id: str):
        """Retrieves dynamobdb item corresponding with request_id in specified table
        Input:
            table: (DynamoTable) enum
            request_id: (str) request id key in table
        Output:
            item: dynamodb item
        """
        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        item = dynamo_table.get_item(
            Key={"RequestId": request_id},
            ConsistentRead=True
        )['Item']
        return item

    def increment_table_field(self, table: DynamoTable, request_id: str, field_name: str, increment_size: int):
        """Increment value in dynamo table
        Args:
            table: DynamoTable enum
            request_id: request id key in table
            field_name: Name of the field to increment
            increment_size: Amount by which to increment the field.
        Returns:
            start_value, end_value: The values before and after incrementing
        """
        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        key_dict = {"RequestId": request_id}
        start_value, end_value = self._increment_field(dynamo_table, key_dict, field_name, increment_size)
        return start_value, end_value

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
            item = db_response['Item']
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
                if exc.response['Error']['Code'] == "ConditionalCheckFailedException":
                    pass
                else:
                    raise
            time.sleep(.5)

        return start_value, new_value

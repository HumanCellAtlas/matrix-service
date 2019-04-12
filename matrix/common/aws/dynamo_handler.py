import os
import time
from enum import Enum

import boto3
import botocore
import requests

from matrix.common.exceptions import MatrixException
from matrix.common import date


class TableField(Enum):
    pass


class StateTableField(TableField):
    """
    Field names for State table in DynamoDB.
    """
    REQUEST_ID = "RequestId"
    CREATION_DATE = "CreationDate"
    EXPECTED_DRIVER_EXECUTIONS = "ExpectedDriverExecutions"
    COMPLETED_DRIVER_EXECUTIONS = "CompletedDriverExecutions"
    EXPECTED_QUERY_EXECUTIONS = "ExpectedQueryExecutions"
    COMPLETED_QUERY_EXECUTIONS = "CompletedQueryExecutions"
    EXPECTED_CONVERTER_EXECUTIONS = "ExpectedConverterExecutions"
    COMPLETED_CONVERTER_EXECUTIONS = "CompletedConverterExecutions"
    BATCH_JOB_ID = "BatchJobId"


class OutputTableField(TableField):
    """
    Field names for Output table in DynamoDB.
    """
    REQUEST_ID = "RequestId"
    NUM_BUNDLES = "NumBundles"
    ROW_COUNT = "RowCount"
    FORMAT = "Format"
    ERROR_MESSAGE = "ErrorMessage"


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
        if dynamo_table == DynamoTable.STATE_TABLE:
            return self._state_table
        elif dynamo_table == DynamoTable.OUTPUT_TABLE:
            return self._output_table

    def create_state_table_entry(self,
                                 request_id: str):
        """
        Put a new item in the DynamoDB table responsible for tracking task execution states and
        counts for a specified request.

        :param request_id: UUID identifying a matrix service request.
        :param format: User requested output file format of final expression matrix.
        """

        self._state_table.put_item(
            Item={
                StateTableField.REQUEST_ID.value: request_id,
                StateTableField.EXPECTED_DRIVER_EXECUTIONS.value: 1,
                StateTableField.COMPLETED_DRIVER_EXECUTIONS.value: 0,
                StateTableField.EXPECTED_QUERY_EXECUTIONS.value: 3,
                StateTableField.COMPLETED_QUERY_EXECUTIONS.value: 0,
                StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value: 1,
                StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value: 0,
                StateTableField.CREATION_DATE.value: date.get_datetime_now(as_string=True),
                StateTableField.BATCH_JOB_ID.value: "N/A"
            }
        )

    def create_output_table_entry(self, request_id: str, num_bundles: int, format: str):
        """
        Put a new item in the DynamoDB Table responsible for counting output rows

        :param request_id: UUID identifying a matrix service request.
        :param num_bundles: the number of bundles in the request.
        :param format: expected file format for matrix service request.
        """
        self._output_table.put_item(
            Item={
                OutputTableField.REQUEST_ID.value: request_id,
                OutputTableField.NUM_BUNDLES.value: num_bundles,
                OutputTableField.ROW_COUNT.value: 0,
                OutputTableField.FORMAT.value: format,
                OutputTableField.ERROR_MESSAGE.value: 0,
            }
        )

    def get_table_item(self, table: DynamoTable, request_id: str=""):
        """Retrieves dynamobdb item corresponding with request_id in the specified table.

        Input:
            table: (DynamoTable) enum
            request_id: (str) request id key in table
        Output:
            item: dynamodb item
        """

        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        try:
            table_key = {'RequestId': request_id}
            item = dynamo_table.get_item(
                Key=table_key,
                ConsistentRead=True
            )['Item']
        except KeyError:
            raise MatrixException(status=requests.codes.not_found,
                                  title=f"Unable to find table item with request ID "
                                        f"{request_id} from DynamoDb Table {table.value}.")

        return item

    def increment_table_field(self, table: DynamoTable, request_id: str, field_enum: TableField, increment_size: int):
        """Increment value in dynamo table
        Args:
            table: DynamoTable enum
            request_id: request id key in table
            field_enum: field enum to increment
            increment_size: Amount by which to increment the field.
        Returns:
            start_value, end_value: The values before and after incrementing
        """
        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        key_dict = {"RequestId": request_id}
        start_value, end_value = self._increment_field(dynamo_table, key_dict, field_enum, increment_size)
        return start_value, end_value

    def set_table_field_with_value(self, table: DynamoTable, request_id: str, field_enum: TableField, field_value: str):
        """
        Set value in dynamo table
        Args:
            table: DynamoTable enum
            request_id: request id key in table
            field_enum: field enum to increment
            field_value: Value to set for field
        """
        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        key_dict = {"RequestId": request_id}
        self._set_field(dynamo_table, key_dict, field_enum, field_value)

    def _set_field(self, table, key_dict: dict, field_enum: TableField, field_value: str):
        """
        Set a value in a dynamo table.
        Args:
          table: boto3 resource for a dynamodb table
          key_dict: Dict for the key in the table
          field_enum: Name of the field to increment
          field_value: Value to set for field
        """
        field_enum_value = field_enum.value
        while True:
            try:
                table.update_item(
                    Key=key_dict,
                    UpdateExpression=f"SET {field_enum_value} = :n",
                    ExpressionAttributeValues={":n": field_value}
                )
                break
            except botocore.exceptions.ClientError as exc:
                if exc.response['Error']['Code'] == "ConditionalCheckFailedException":
                    pass
                else:
                    raise
            time.sleep(.5)

    def _increment_field(self, table, key_dict: dict, field_enum: TableField, increment_size: int):
        """Increment a value in a dynamo table safely.
        Makes sure distributed table updates don't clobber each other. For example,
        increment_field(dynamo_table_obj, {"id": id_}, "Counts", 5)
        will increment the Counts value in the item keyed by {"id": id_} in table
        "my_table" by 5.
        Args:
          table: boto3 resource for a dynamodb table
          key_dict: Dict for the key in the table
          field_value: Name of the field to increment
          increment_size: Amount by which to increment the field.
        Returns:
          start_value, end_value: The values before and after incrementing
        """
        field_value = field_enum.value
        while True:
            db_response = table.get_item(
                Key=key_dict,
                ConsistentRead=True
            )
            item = db_response['Item']
            start_value = item[field_value]
            new_value = start_value + increment_size

            try:
                table.update_item(
                    Key=key_dict,
                    UpdateExpression=f"SET {field_value} = :n",
                    ConditionExpression=f"{field_value} = :s",
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

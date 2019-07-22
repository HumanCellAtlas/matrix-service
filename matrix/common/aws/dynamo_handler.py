import os
import time
import typing
from enum import Enum

import boto3
import botocore
import requests

from matrix.common.exceptions import MatrixException
from matrix.common import date


class TableField(Enum):
    pass


class RequestTableField(TableField):
    """
    Field names for Request table in DynamoDB.
    """
    REQUEST_ID = "RequestId"
    CREATION_DATE = "CreationDate"
    FORMAT = "Format"
    NUM_BUNDLES = "NumBundles"
    ROW_COUNT = "RowCount"
    EXPECTED_DRIVER_EXECUTIONS = "ExpectedDriverExecutions"
    COMPLETED_DRIVER_EXECUTIONS = "CompletedDriverExecutions"
    EXPECTED_QUERY_EXECUTIONS = "ExpectedQueryExecutions"
    COMPLETED_QUERY_EXECUTIONS = "CompletedQueryExecutions"
    EXPECTED_CONVERTER_EXECUTIONS = "ExpectedConverterExecutions"
    COMPLETED_CONVERTER_EXECUTIONS = "CompletedConverterExecutions"
    BATCH_JOB_ID = "BatchJobId"
    ERROR_MESSAGE = "ErrorMessage"


class DynamoTable(Enum):
    """
    Names of dynamo tables in matrix service
    """
    REQUEST_TABLE = os.getenv("DYNAMO_REQUEST_TABLE_NAME")
    DATA_TABLE = os.getenv("DYNAMO_DATA_TABLE_NAME")


class DynamoHandler:
    """
    Interface for interacting with DynamoDB Tables.
    """
    def __init__(self):
        self._dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self._request_table = self._dynamo.Table(DynamoTable.REQUEST_TABLE.value)
        self._data_table = self._dynamo.Table(DynamoTable.DATA_TABLE.value)

        # Stores access information to Dynamo resources
        self.tables = {
            DynamoTable.REQUEST_TABLE: {
                'key_name': "RequestId",
                'resource': self._request_table
            },
            DynamoTable.DATA_TABLE: {
                'key_name': "Version",
                'resource': self._data_table
            }
        }

    def create_request_table_entry(self,
                                   request_id: str,
                                   fmt: str):
        """
        Put a new item in the Request table responsible for tracking the inputs, task execution progress and errors
        of a Matrix Request.

        :param request_id: UUID identifying a matrix service request.
        :param fmt: User requested output file format of final expression matrix.
        """

        self._request_table.put_item(
            Item={
                RequestTableField.REQUEST_ID.value: request_id,
                RequestTableField.CREATION_DATE.value: date.get_datetime_now(as_string=True),
                RequestTableField.FORMAT.value: fmt,
                RequestTableField.NUM_BUNDLES.value: -1,
                RequestTableField.ROW_COUNT.value: 0,
                RequestTableField.EXPECTED_DRIVER_EXECUTIONS.value: 1,
                RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value: 0,
                RequestTableField.EXPECTED_QUERY_EXECUTIONS.value: 3,
                RequestTableField.COMPLETED_QUERY_EXECUTIONS.value: 0,
                RequestTableField.EXPECTED_CONVERTER_EXECUTIONS.value: 1,
                RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value: 0,
                RequestTableField.BATCH_JOB_ID.value: "N/A",
                RequestTableField.ERROR_MESSAGE.value: 0
            }
        )

    def get_table_item(self, table: DynamoTable, key: str=""):
        """Retrieves dynamobdb item with corresponding primary key in the specified table.

        Input:
            table: (DynamoTable) enum
            key: (str) primary key in table
        Output:
            item: dynamodb item
        """

        dynamo_table = self.tables[table]['resource']
        key_name = self.tables[table]['key_name']
        try:
            item = dynamo_table.get_item(
                Key={key_name: key},
                ConsistentRead=True
            )['Item']
        except KeyError:
            raise MatrixException(status=requests.codes.not_found,
                                  title=f"Unable to find table item with primary key "
                                        f"{key_name} from DynamoDb Table {table.value}.")

        return item

    def increment_table_field(self, table: DynamoTable, key: str, field_enum: TableField, increment_size: int):
        """Increment value in dynamo table
        Args:
            table: DynamoTable enum
            key: primary key in table
            field_enum: field enum to increment
            increment_size: Amount by which to increment the field.
        Returns:
            start_value, end_value: The values before and after incrementing
        """
        dynamo_table = self.tables[table]['resource']
        key_dict = {self.tables[table]['key_name']: key}
        start_value, end_value = self._increment_field(dynamo_table, key_dict, field_enum, increment_size)
        return start_value, end_value

    def set_table_field_with_value(self,
                                   table: DynamoTable,
                                   key: str,
                                   field_enum: TableField,
                                   field_value: typing.Union[str, int]):
        """
        Set value in dynamo table
        Args:
            table: DynamoTable enum
            key: primary key in table
            field_enum: field enum to increment
            field_value: Value to set for field
        """
        dynamo_table = self.tables[table]['resource']
        key_dict = {self.tables[table]['key_name']: key}
        self._set_field(dynamo_table, key_dict, field_enum, field_value)

    def _set_field(self, table, key_dict: dict, field_enum: TableField, field_value: typing.Union[str, int]):
        """
        Set a value in a dynamo table.
        Args:
          table: boto3 resource for a dynamodb table
          key_dict: Dict for the key in the table
          field_enum: Name of the field to increment
          field_value: Value to set for field
        """
        field_enum_value = field_enum.value
        table.update_item(
            Key=key_dict,
            UpdateExpression=f"SET {field_enum_value} = :n",
            ExpressionAttributeValues={":n": field_value}
        )

    def _increment_field(self, table, key_dict: dict, field_enum: TableField, increment_size: int):
        """Increment a value in a dynamo table safely.
        Makes sure distributed table updates don't clobber each other. For example,
        increment_field(dynamo_table_obj, {"id": id_}, "Counts", 5)
        will increment the Counts value in the item keyed by {"id": id_} in table
        "my_table" by 5.
        Args:
          table: boto3 resource for a dynamodb table
          key_dict: Dict for the key in the table
          field_enum: Name of the field to increment
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

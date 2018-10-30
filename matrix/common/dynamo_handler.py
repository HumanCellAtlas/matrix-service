import os
import time
from enum import Enum

import boto3
import botocore
import requests

from matrix.common.constants import MatrixFormat
from matrix.common.exceptions import MatrixException


class TableField(Enum):
    pass


class StateTableField(TableField):
    """
    Field names for State table in DynamoDB.
    """
    REQUEST_HASH = "RequestHash"
    EXPECTED_DRIVER_EXECUTIONS = "ExpectedDriverExecutions"
    COMPLETED_DRIVER_EXECUTIONS = "CompletedDriverExecutions"
    EXPECTED_MAPPER_EXECUTIONS = "ExpectedMapperExecutions"
    COMPLETED_MAPPER_EXECUTIONS = "CompletedMapperExecutions"
    EXPECTED_WORKER_EXECUTIONS = "ExpectedWorkerExecutions"
    COMPLETED_WORKER_EXECUTIONS = "CompletedWorkerExecutions"
    EXPECTED_REDUCER_EXECUTIONS = "ExpectedReducerExecutions"
    COMPLETED_REDUCER_EXECUTIONS = "CompletedReducerExecutions"
    EXPECTED_CONVERTER_EXECUTIONS = "ExpectedConverterExecutions"
    COMPLETED_CONVERTER_EXECUTIONS = "CompletedConverterExecutions"


class OutputTableField(TableField):
    """
    Field names for Output table in DynamoDB.
    """
    REQUEST_HASH = "RequestHash"
    ROW_COUNT = "RowCount"
    FORMAT = "Format"
    ERROR_MESSAGE = "ErrorMessage"


class CacheTableField(TableField):
    """
    Field names for the cache table in DynamoDB.
    """
    REQUEST_ID = "RequestId"
    REQUEST_HASH = "RequestHash"


class DynamoTable(Enum):
    """
    Names of dynamo tables in matrix service
    """
    STATE_TABLE = os.getenv("DYNAMO_STATE_TABLE_NAME")
    OUTPUT_TABLE = os.getenv("DYNAMO_OUTPUT_TABLE_NAME")
    CACHE_TABLE = os.getenv("DYNAMO_CACHE_TABLE_NAME")


class DynamoHandler:
    """
    Interface for interacting with DynamoDB Tables.
    """
    def __init__(self):
        self._dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self._state_table = self._dynamo.Table(DynamoTable.STATE_TABLE.value)
        self._output_table = self._dynamo.Table(DynamoTable.OUTPUT_TABLE.value)
        self._cache_table = self._dynamo.Table(DynamoTable.CACHE_TABLE.value)

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
                                 request_hash: str,
                                 num_mappers: int,
                                 format: str=MatrixFormat.ZARR.value):
        """
        Put a new item in the DynamoDB table responsible for tracking task execution states and
        counts for a specified request.

        :param request_hash: UUID identifying a filter merge job request.
        :param num_mappers: Number of mapper lambdas expected to be invoked.
        :param format: User requested output file format of final expression matrix.
        """

        self._state_table.put_item(
            Item={
                StateTableField.REQUEST_HASH.value: request_hash,
                StateTableField.EXPECTED_DRIVER_EXECUTIONS.value: 1,
                StateTableField.COMPLETED_DRIVER_EXECUTIONS.value: 0,
                StateTableField.EXPECTED_MAPPER_EXECUTIONS.value: num_mappers,
                StateTableField.COMPLETED_MAPPER_EXECUTIONS.value: 0,
                StateTableField.EXPECTED_WORKER_EXECUTIONS.value: 0,
                StateTableField.COMPLETED_WORKER_EXECUTIONS.value: 0,
                StateTableField.EXPECTED_REDUCER_EXECUTIONS.value: 1,
                StateTableField.COMPLETED_REDUCER_EXECUTIONS.value: 0,
                StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value: 0 if format == MatrixFormat.ZARR.value else 1,
                StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value: 0,
            }
        )

    def create_output_table_entry(self, request_hash: str, format: str):
        """
        Put a new item in the DynamoDB Table responsible for counting output rows

        :param request_hash: UUID identifying a filter merge job request.
        :param format: expected file format for filter merge job request.
        """
        self._output_table.put_item(
            Item={
                OutputTableField.REQUEST_HASH.value: request_hash,
                OutputTableField.ROW_COUNT.value: 0,
                OutputTableField.FORMAT.value: format,
                OutputTableField.ERROR_MESSAGE.value: 0,
            }
        )

    def write_request_error(self, request_hash: str, message: str):
        """
        Write an error message a request's DyanmoDB Output table.
        :param request_hash: str The request ID of the request that reported the error
        :param message: str The error message
        """
        self._output_table.update_item(
            Key={OutputTableField.REQUEST_HASH.value: request_hash},
            UpdateExpression=f"SET {OutputTableField.ERROR_MESSAGE.value} = :m",
            ExpressionAttributeValues={':m': message}
        )

    def get_table_item(self, table: DynamoTable, request_hash: str):
        """Retrieves dynamobdb item corresponding with request_hash in specified table
        Input:
            table: (DynamoTable) enum
            request_hash: (str) request id key in table
        Output:
            item: dynamodb item
        """
        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        try:
            item = dynamo_table.get_item(
                Key={'RequestHash': request_hash},
                ConsistentRead=True
            )['Item']
        except KeyError:
            raise MatrixException(status=requests.codes.not_found,
                                  title=f"Unable to find table item with request ID "
                                        f"{request_hash} from DynamoDb Table {table.value}.")

        return item

    def increment_table_field(self, table: DynamoTable, request_hash: str, field_enum: TableField, increment_size: int):
        """Increment value in dynamo table
        Args:
            table: DynamoTable enum
            request_hash: request id key in table
            field_enum: field enum to increment
            increment_size: Amount by which to increment the field.
        Returns:
            start_value, end_value: The values before and after incrementing
        """
        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        key_dict = {"RequestHash": request_hash}
        start_value, end_value = self._increment_field(dynamo_table, key_dict, field_enum, increment_size)
        return start_value, end_value

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

    def get_request_hash(self, request_id):
        """Checks if the hashed request appears in the cache table. If so, return the
        associated request id. If not, return None.

        Input:
            request_id: (int) hash of the request
        Output:
            request_hash: (str or None) previously generated request id for the request
        """
        try:
            item = self._cache_table.get_item(
                Key={CacheTableField.REQUEST_ID.value: request_id},
                ConsistentRead=True
            )['Item']
        except KeyError:
            return None

        return item[CacheTableField.REQUEST_HASH.value]

    def write_request_hash(self, request_id, request_hash):
        """Write a new entry in the cache table for a request

        Input:
            request_hash: (int) hash of the request
            request_id: (uuid) request id to associate with the hash
        """
        self._cache_table.put_item(
            Item={
                CacheTableField.REQUEST_ID.value: request_id,
                CacheTableField.REQUEST_HASH.value: request_hash
            }
        )

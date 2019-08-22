import os
import time
import typing
from enum import Enum

import boto3
import botocore
import requests

from matrix.common import date
from matrix.common.constants import DEFAULT_FEATURE, DEFAULT_FIELDS, SUPPORTED_METADATA_SCHEMA_VERSIONS
from matrix.common.exceptions import MatrixException
from matrix.common.v1_api_handler import V1ApiHandler


class DynamoTable(Enum):
    """
    Names of dynamo tables in matrix service
    """
    DATA_VERSION_TABLE = os.getenv("DYNAMO_DATA_VERSION_TABLE_NAME")
    DEPLOYMENT_TABLE = os.getenv("DYNAMO_DEPLOYMENT_TABLE_NAME")
    REQUEST_TABLE = os.getenv("DYNAMO_REQUEST_TABLE_NAME")


class TableField(Enum):
    pass


class DataVersionTableField(TableField):
    """
    Field names for Deployment table in DynamoDB.
    """
    DATA_VERSION = "DataVersion"
    CREATION_DATE = "CreationDate"
    PROJECT_CELL_COUNTS = "ProjectCellCounts"
    METADATA_SCHEMA_VERSIONS = "MetadataSchemaVersions"


class DeploymentTableField(TableField):
    """
    Field names for Deployment table in DynamoDB.
    """
    DEPLOYMENT = "Deployment"
    CURRENT_DATA_VERSION = "CurrentDataVersion"


class RequestTableField(TableField):
    """
    Field names for Request table in DynamoDB.
    """
    REQUEST_ID = "RequestId"
    REQUEST_HASH = "RequestHash"
    DATA_VERSION = "DataVersion"
    CREATION_DATE = "CreationDate"
    FORMAT = "Format"
    METADATA_FIELDS = "MetadataFields"
    FEATURE = "Feature"
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


class DynamoHandler:
    """
    Interface for interacting with DynamoDB Tables.
    """

    def __init__(self):
        self._dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self.tables = {
            DynamoTable.DATA_VERSION_TABLE: {
                'primary_key': DataVersionTableField.DATA_VERSION.value,
                'resource': self._dynamo.Table(DynamoTable.DATA_VERSION_TABLE.value)
            },
            DynamoTable.DEPLOYMENT_TABLE: {
                'primary_key': DeploymentTableField.DEPLOYMENT.value,
                'resource': self._dynamo.Table(DynamoTable.DEPLOYMENT_TABLE.value)
            },
            DynamoTable.REQUEST_TABLE: {
                'primary_key': RequestTableField.REQUEST_ID.value,
                'resource': self._dynamo.Table(DynamoTable.REQUEST_TABLE.value)
            }
        }

    def _get_dynamo_table_resource_from_enum(self, dynamo_table: DynamoTable):
        """Retrieve dynamo table resource for a given dynamo table name.

        Input:
            dynamo_table: (DynamoTable), Enum
        Output:
            boto3 dynamodb resource
        """
        return self.tables[dynamo_table]['resource']

    def _get_dynamo_table_primary_key_from_enum(self, dynamo_table: DynamoTable):
        """Retrieve dynamo table primary key for a given dynamo table name.

        Input:
            dynamo_table: (DynamoTable), Enum
        Output:
            str Primary key
        """
        return self.tables[dynamo_table]['primary_key']

    def create_data_version_table_entry(self, version: int):
        """
        Put a new item in the Data Version table responsible for describing the current and
        previous Redshift data versions for a deployment.
        If the new version already exists, it will be overwritten by the new entry.
        :param version: Version number to create
        """
        api_handler = V1ApiHandler()
        project_cell_counts = api_handler.describe_filter("project.provenance.document_id")['cell_counts']

        metadata_schema_versions = {}
        for schema_name in SUPPORTED_METADATA_SCHEMA_VERSIONS:
            metadata_schema_versions[schema_name.value] = SUPPORTED_METADATA_SCHEMA_VERSIONS[schema_name]

        self._get_dynamo_table_resource_from_enum(DynamoTable.DATA_VERSION_TABLE).put_item(
            Item={
                DataVersionTableField.DATA_VERSION.value: version,
                DataVersionTableField.CREATION_DATE.value: date.get_datetime_now(as_string=True),
                DataVersionTableField.PROJECT_CELL_COUNTS.value: project_cell_counts,
                DataVersionTableField.METADATA_SCHEMA_VERSIONS.value: metadata_schema_versions
            }
        )

    def create_request_table_entry(self,
                                   request_id: str,
                                   fmt: str,
                                   metadata_fields: list = DEFAULT_FIELDS,
                                   feature: str = DEFAULT_FEATURE):
        """
        Put a new item in the Request table responsible for tracking the inputs, task execution progress and errors
        of a Matrix Request.

        :param request_id: UUID identifying a matrix service request.
        :param fmt: User requested output file format of final expression matrix.
        :param metadata_fields: User requested metadata fields to include in the expression matrix.
        :param feature: User requested feature type of final expression matrix (gene|transcript).
        """
        data_version = \
            self.get_table_item(table=DynamoTable.DEPLOYMENT_TABLE,
                                key=os.environ['DEPLOYMENT_STAGE'])[DeploymentTableField.CURRENT_DATA_VERSION.value]

        self._get_dynamo_table_resource_from_enum(DynamoTable.REQUEST_TABLE).put_item(
            Item={
                RequestTableField.REQUEST_ID.value: request_id,
                RequestTableField.REQUEST_HASH.value: "N/A",
                RequestTableField.DATA_VERSION.value: data_version,
                RequestTableField.CREATION_DATE.value: date.get_datetime_now(as_string=True),
                RequestTableField.FORMAT.value: fmt,
                RequestTableField.METADATA_FIELDS.value: metadata_fields,
                RequestTableField.FEATURE.value: feature,
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

    def get_table_item(self, table: DynamoTable, key: str = ""):
        """Retrieves dynamobdb item corresponding with primary key in the specified table.

        Input:
            table: (DynamoTable) enum
            key: (str) primary key in table
        Output:
            item: dynamodb item
        """

        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        try:
            table_key = {self._get_dynamo_table_primary_key_from_enum(table): key}
            item = dynamo_table.get_item(
                Key=table_key,
                ConsistentRead=True
            )['Item']
        except KeyError:
            raise MatrixException(status=requests.codes.not_found,
                                  title=f"Unable to find table item with key "
                                  f"{key} from DynamoDb Table {table.value}.")

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
        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        key_dict = {self._get_dynamo_table_primary_key_from_enum(table): key}
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
        dynamo_table = self._get_dynamo_table_resource_from_enum(table)
        key_dict = {self._get_dynamo_table_primary_key_from_enum(table): key}
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

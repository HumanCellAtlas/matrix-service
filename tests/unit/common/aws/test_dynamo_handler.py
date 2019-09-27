import os
import uuid
import mock

import boto3

from matrix.common.constants import DEFAULT_FEATURE, DEFAULT_FIELDS, GenusSpecies, SUPPORTED_METADATA_SCHEMA_VERSIONS
from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, RequestTableField, DataVersionTableField
from matrix.common.exceptions import MatrixException
from tests.unit import MatrixTestCaseUsingMockAWS


class TestDynamoHandler(MatrixTestCaseUsingMockAWS):
    """
    Environment variables are set in tests/unit/__init__.py
    """
    def setUp(self):
        super(TestDynamoHandler, self).setUp()

        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self.data_version_table_name = os.environ['DYNAMO_DATA_VERSION_TABLE_NAME']
        self.request_table_name = os.environ['DYNAMO_REQUEST_TABLE_NAME']
        self.request_id = str(uuid.uuid4())
        self.data_version = 1
        self.format = "zarr"

        self.create_test_data_version_table()
        self.create_test_deployment_table()
        self.create_test_request_table()

        self.init_test_data_version_table()
        self.init_test_deployment_table()

        self.handler = DynamoHandler()

    def _get_data_version_table_response_and_entry(self):
        data_version_primary_key = self.handler._get_dynamo_table_primary_key_from_enum(DynamoTable.DATA_VERSION_TABLE)
        response = self.dynamo.batch_get_item(
            RequestItems={
                self.data_version_table_name: {
                    'Keys': [{data_version_primary_key: self.data_version}]
                }
            }
        )
        entry = response['Responses'][self.data_version_table_name][0]
        return response, entry

    def _get_request_table_response_and_entry(self):
        response = self.dynamo.batch_get_item(
            RequestItems={
                self.request_table_name: {
                    'Keys': [{'RequestId': self.request_id}]
                }
            }
        )
        entry = response['Responses'][self.request_table_name][0]
        return response, entry

    @mock.patch("matrix.common.v1_api_handler.V1ApiHandler.describe_filter")
    @mock.patch("matrix.common.date.get_datetime_now")
    def test_create_data_version_table_entry(self, mock_get_datetime_now, mock_describe_filter):
        stub_date = '2019-03-18T180907.136216Z'
        mock_get_datetime_now.return_value = stub_date

        stub_cell_counts = {
            'test_project_uuid_1': 10,
            'test_project_uuid_2': 100
        }
        mock_describe_filter.return_value = {
            'cell_counts': stub_cell_counts
        }

        self.handler.create_data_version_table_entry(self.data_version)
        response, entry = self._get_data_version_table_response_and_entry()

        metadata_schema_versions = {}
        for schema_name in SUPPORTED_METADATA_SCHEMA_VERSIONS:
            metadata_schema_versions[schema_name.value] = SUPPORTED_METADATA_SCHEMA_VERSIONS[schema_name]

        self.assertEqual(len(response['Responses'][self.data_version_table_name]), 1)

        self.assertTrue(all(field.value in entry for field in DataVersionTableField))
        self.assertEqual(entry[DataVersionTableField.DATA_VERSION.value], self.data_version)
        self.assertEqual(entry[DataVersionTableField.CREATION_DATE.value], stub_date)
        self.assertEqual(entry[DataVersionTableField.PROJECT_CELL_COUNTS.value], stub_cell_counts)
        self.assertEqual(entry[DataVersionTableField.METADATA_SCHEMA_VERSIONS.value], metadata_schema_versions)

    @mock.patch("matrix.common.date.get_datetime_now")
    def test_create_request_table_entry(self, mock_get_datetime_now):
        stub_date = '2019-03-18T180907.136216Z'
        mock_get_datetime_now.return_value = stub_date
        self.handler.create_request_table_entry(self.request_id, self.format, [GenusSpecies.HUMAN])
        response, entry = self._get_request_table_response_and_entry()

        self.assertEqual(len(response['Responses'][self.request_table_name]), 1)

        self.assertTrue(all(field.value in entry for field in RequestTableField))
        self.assertEqual(entry[RequestTableField.FORMAT.value], self.format)
        self.assertEqual(entry[RequestTableField.METADATA_FIELDS.value], DEFAULT_FIELDS)
        self.assertEqual(entry[RequestTableField.FEATURE.value], "gene")
        self.assertEqual(entry[RequestTableField.DATA_VERSION.value], 0)
        self.assertEqual(entry[RequestTableField.REQUEST_HASH.value], {GenusSpecies.HUMAN.value: "N/A"})
        self.assertEqual(entry[RequestTableField.EXPECTED_DRIVER_EXECUTIONS.value], 1)
        self.assertEqual(entry[RequestTableField.EXPECTED_QUERY_EXECUTIONS.value], {GenusSpecies.HUMAN.value: 3})
        self.assertEqual(entry[RequestTableField.EXPECTED_CONVERTER_EXECUTIONS.value], {GenusSpecies.HUMAN.value: 1})
        self.assertEqual(entry[RequestTableField.CREATION_DATE.value], stub_date)

    def test_increment_table_field_request_table_path(self):
        self.handler.create_request_table_entry(self.request_id, self.format, [GenusSpecies.HUMAN, GenusSpecies.MOUSE])

        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 0)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value],
                         {GenusSpecies.HUMAN.value: 0, GenusSpecies.MOUSE.value: 0})

        field_enum = RequestTableField.COMPLETED_DRIVER_EXECUTIONS
        self.handler.increment_table_field(DynamoTable.REQUEST_TABLE, self.request_id, field_enum, 5)
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 5)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value],
                         {GenusSpecies.HUMAN.value: 0, GenusSpecies.MOUSE.value: 0})

    def test_set_table_field_with_value(self):
        self.handler.create_request_table_entry(self.request_id, self.format, [GenusSpecies.HUMAN])
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.FORMAT.value], self.format)

        field_enum = RequestTableField.FORMAT
        self.handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE, self.request_id, field_enum, "mtx")
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.FORMAT.value], "mtx")

    def test_update_table_dict_field(self):
        self.handler.create_request_table_entry(self.request_id, self.format,
                                                [GenusSpecies.HUMAN, GenusSpecies.MOUSE])
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.BATCH_JOB_ID.value],
                         {GenusSpecies.HUMAN.value: "N/A", GenusSpecies.MOUSE.value: "N/A"})
        field = RequestTableField.BATCH_JOB_ID
        self.handler.update_table_dict_field(DynamoTable.REQUEST_TABLE, self.request_id,
                                             field, GenusSpecies.HUMAN.value, "abc123")
        response, entry = self._get_request_table_response_and_entry()
        self.assertDictEqual(entry[RequestTableField.BATCH_JOB_ID.value],
                             {GenusSpecies.HUMAN.value: "abc123", GenusSpecies.MOUSE.value: "N/A"})

    def test_increment_field(self):
        self.handler.create_request_table_entry(self.request_id, self.format, [GenusSpecies.HUMAN])
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 0)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value], {GenusSpecies.HUMAN.value: 0})

        key_dict = {"RequestId": self.request_id}
        field_enum = RequestTableField.COMPLETED_DRIVER_EXECUTIONS
        self.handler._increment_field(self.handler._get_dynamo_table_resource_from_enum(DynamoTable.REQUEST_TABLE),
                                      key_dict,
                                      field_enum,
                                      15)
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 15)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value], {GenusSpecies.HUMAN.value: 0})

    def test_increment_field_key(self):
        self.handler.create_request_table_entry(self.request_id, self.format, [GenusSpecies.MOUSE])
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 0)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value], {GenusSpecies.MOUSE.value: 0})

        key_dict = {"RequestId": self.request_id}
        field_enum = RequestTableField.COMPLETED_CONVERTER_EXECUTIONS
        self.handler._increment_field(self.handler._get_dynamo_table_resource_from_enum(DynamoTable.REQUEST_TABLE),
                                      key_dict,
                                      field_enum,
                                      5,
                                      GenusSpecies.MOUSE.value)
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 0)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value], {GenusSpecies.MOUSE.value: 5})

    def test_set_field(self):
        self.handler.create_request_table_entry(self.request_id, self.format, [GenusSpecies.HUMAN])
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.BATCH_JOB_ID.value], {GenusSpecies.HUMAN.value: "N/A"})

        key_dict = {"RequestId": self.request_id}
        field_enum = RequestTableField.BATCH_JOB_ID
        self.handler._set_field(self.handler._get_dynamo_table_resource_from_enum(DynamoTable.REQUEST_TABLE),
                                key_dict,
                                field_enum,
                                "123-123")
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.BATCH_JOB_ID.value], "123-123")

    def test_get_request_table_entry(self):
        self.handler.create_request_table_entry(self.request_id, self.format, [GenusSpecies.HUMAN])
        entry = self.handler.get_table_item(DynamoTable.REQUEST_TABLE, key=self.request_id)
        self.assertEqual(entry[RequestTableField.EXPECTED_DRIVER_EXECUTIONS.value], 1)

        key_dict = {"RequestId": self.request_id}
        field_enum = RequestTableField.COMPLETED_DRIVER_EXECUTIONS
        self.handler._increment_field(self.handler._get_dynamo_table_resource_from_enum(DynamoTable.REQUEST_TABLE),
                                      key_dict,
                                      field_enum,
                                      15)
        entry = self.handler.get_table_item(DynamoTable.REQUEST_TABLE, key=self.request_id)
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 15)

    def test_get_table_item(self):
        self.assertRaises(MatrixException, self.handler.get_table_item,
                          DynamoTable.REQUEST_TABLE,
                          key=self.request_id)

        self.handler.create_request_table_entry(self.request_id, self.format, [GenusSpecies.HUMAN])
        entry = self.handler.get_table_item(DynamoTable.REQUEST_TABLE, key=self.request_id)
        self.assertEqual(entry[RequestTableField.ROW_COUNT.value], 0)

    def test_filter_table_items(self):
        items = self.handler.filter_table_items(
            table=DynamoTable.REQUEST_TABLE,
            attrs={RequestTableField.FEATURE.value: DEFAULT_FEATURE}
        )
        self.assertEqual(len(items), 0)

        self.handler.create_request_table_entry(self.request_id, self.format, [GenusSpecies.HUMAN, GenusSpecies.MOUSE])
        # Add a lot of entries to make sure we test the pagination
        for _ in range(250):
            self.handler.create_request_table_entry(str(uuid.uuid4()), "test_format", [GenusSpecies.HUMAN])
        self.handler.create_request_table_entry(str(uuid.uuid4()), self.format, [GenusSpecies.MOUSE])

        items = self.handler.filter_table_items(
            table=DynamoTable.REQUEST_TABLE,
            attrs={RequestTableField.FEATURE.value: DEFAULT_FEATURE}
        )
        self.assertEqual(len(items), 252)

        items = self.handler.filter_table_items(
            table=DynamoTable.REQUEST_TABLE,
            attrs={RequestTableField.REQUEST_HASH.value: {GenusSpecies.HUMAN.value: "N/A"}}
        )
        self.assertEqual(len(items), 250)

        items = self.handler.filter_table_items(
            table=DynamoTable.REQUEST_TABLE,
            attrs={RequestTableField.REQUEST_HASH.value: {GenusSpecies.MOUSE.value: "N/A"}}
        )
        self.assertEqual(len(items), 1)

        items = self.handler.filter_table_items(
            table=DynamoTable.REQUEST_TABLE,
            attrs={RequestTableField.REQUEST_HASH.value: {GenusSpecies.MOUSE.value: "N/A",
                                                          GenusSpecies.HUMAN.value: "N/A"}}
        )
        self.assertEqual(len(items), 1)

        items = self.handler.filter_table_items(
            table=DynamoTable.REQUEST_TABLE,
            attrs={RequestTableField.FEATURE.value: DEFAULT_FEATURE,
                   RequestTableField.FORMAT.value: self.format}
        )
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0][RequestTableField.REQUEST_ID.value], self.request_id)

    def test_filter_table_dict_value(self):

        for _ in range(75):
            self.handler.create_request_table_entry(str(uuid.uuid4()), "test_format", [GenusSpecies.HUMAN])

        items = self.handler.filter_table_dict_value(
            table=DynamoTable.REQUEST_TABLE,
            field=RequestTableField.REQUEST_HASH.value,
            value='N/A'
        )
        self.assertEqual(len(items), 75)

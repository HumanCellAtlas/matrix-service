import os
import uuid
import mock

import boto3

from tests.unit import MatrixTestCaseUsingMockAWS
from matrix.common.aws.dynamo_handler import DynamoHandler
from matrix.common.aws.dynamo_handler import RequestTableField
from matrix.common.aws.dynamo_handler import DynamoTable
from matrix.common.exceptions import MatrixException


class TestDynamoHandler(MatrixTestCaseUsingMockAWS):
    """
    Environment variables are set in tests/unit/__init__.py
    """
    def setUp(self):
        super(TestDynamoHandler, self).setUp()

        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self.request_table_name = os.environ['DYNAMO_REQUEST_TABLE_NAME']
        self.request_id = str(uuid.uuid4())
        self.format = "zarr"

        self.create_test_request_table()

        self.handler = DynamoHandler()

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

    @mock.patch("matrix.common.date.get_datetime_now")
    def test_create_request_table_entry(self, mock_get_datetime_now):
        stub_date = '2019-03-18T180907.136216Z'
        mock_get_datetime_now.return_value = stub_date
        self.handler.create_request_table_entry(self.request_id, self.format)
        response, entry = self._get_request_table_response_and_entry()

        self.assertEqual(len(response['Responses'][self.request_table_name]), 1)

        self.assertTrue(all(field.value in entry for field in RequestTableField))
        self.assertEqual(entry[RequestTableField.FORMAT.value], self.format)
        self.assertEqual(entry[RequestTableField.METADATA_FIELDS.value], [])
        self.assertEqual(entry[RequestTableField.FEATURE.value], "gene")
        self.assertEqual(entry[RequestTableField.DATA_VERSION.value], 0)
        self.assertEqual(entry[RequestTableField.REQUEST_HASH.value], "N/A")
        self.assertEqual(entry[RequestTableField.EXPECTED_DRIVER_EXECUTIONS.value], 1)
        self.assertEqual(entry[RequestTableField.EXPECTED_CONVERTER_EXECUTIONS.value], 1)
        self.assertEqual(entry[RequestTableField.CREATION_DATE.value], stub_date)

    def test_increment_table_field_request_table_path(self):
        self.handler.create_request_table_entry(self.request_id, self.format)

        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 0)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value], 0)

        field_enum = RequestTableField.COMPLETED_DRIVER_EXECUTIONS
        self.handler.increment_table_field(DynamoTable.REQUEST_TABLE, self.request_id, field_enum, 5)
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 5)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value], 0)

    def test_set_table_field_with_value(self):
        self.handler.create_request_table_entry(self.request_id, self.format)
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.BATCH_JOB_ID.value], "N/A")

        field_enum = RequestTableField.BATCH_JOB_ID
        self.handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE, self.request_id, field_enum, "123-123")
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.BATCH_JOB_ID.value], "123-123")

    def test_increment_field(self):
        self.handler.create_request_table_entry(self.request_id, self.format)
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 0)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value], 0)

        key_dict = {"RequestId": self.request_id}
        field_enum = RequestTableField.COMPLETED_DRIVER_EXECUTIONS
        self.handler._increment_field(self.handler._request_table, key_dict, field_enum, 15)
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 15)
        self.assertEqual(entry[RequestTableField.COMPLETED_CONVERTER_EXECUTIONS.value], 0)

    def test_set_field(self):
        self.handler.create_request_table_entry(self.request_id, self.format)
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.BATCH_JOB_ID.value], "N/A")

        key_dict = {"RequestId": self.request_id}
        field_enum = RequestTableField.BATCH_JOB_ID
        self.handler._set_field(self.handler._request_table, key_dict, field_enum, "123-123")
        response, entry = self._get_request_table_response_and_entry()
        self.assertEqual(entry[RequestTableField.BATCH_JOB_ID.value], "123-123")

    def test_get_request_table_entry(self):
        self.handler.create_request_table_entry(self.request_id, self.format)
        entry = self.handler.get_table_item(DynamoTable.REQUEST_TABLE, request_id=self.request_id)
        self.assertEqual(entry[RequestTableField.EXPECTED_DRIVER_EXECUTIONS.value], 1)

        key_dict = {"RequestId": self.request_id}
        field_enum = RequestTableField.COMPLETED_DRIVER_EXECUTIONS
        self.handler._increment_field(self.handler._request_table, key_dict, field_enum, 15)
        entry = self.handler.get_table_item(DynamoTable.REQUEST_TABLE, request_id=self.request_id)
        self.assertEqual(entry[RequestTableField.COMPLETED_DRIVER_EXECUTIONS.value], 15)

    def test_get_table_item(self):
        self.assertRaises(MatrixException, self.handler.get_table_item,
                          DynamoTable.REQUEST_TABLE,
                          request_id=self.request_id)

        self.handler.create_request_table_entry(self.request_id, self.format)
        entry = self.handler.get_table_item(DynamoTable.REQUEST_TABLE, request_id=self.request_id)
        self.assertEqual(entry[RequestTableField.ROW_COUNT.value], 0)

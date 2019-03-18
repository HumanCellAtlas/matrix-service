import os
import uuid
import mock

import boto3

from tests.unit import MatrixTestCaseUsingMockAWS
from matrix.common.aws.dynamo_handler import DynamoHandler
from matrix.common.aws.dynamo_handler import StateTableField
from matrix.common.aws.dynamo_handler import OutputTableField
from matrix.common.aws.dynamo_handler import DynamoTable
from matrix.common.exceptions import MatrixException


class TestDynamoHandler(MatrixTestCaseUsingMockAWS):
    """
    Environment variables are set in tests/unit/__init__.py
    """
    def setUp(self):
        super(TestDynamoHandler, self).setUp()

        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self.state_table_name = os.environ['DYNAMO_STATE_TABLE_NAME']
        self.output_table_name = os.environ['DYNAMO_OUTPUT_TABLE_NAME']
        self.request_id = str(uuid.uuid4())
        self.format = "zarr"

        self.create_test_state_table()
        self.create_test_output_table()

        self.handler = DynamoHandler()

    def _get_state_table_response_and_entry(self):
        response = self.dynamo.batch_get_item(
            RequestItems={
                self.state_table_name: {
                    'Keys': [{'RequestId': self.request_id}]
                }
            }
        )
        entry = response['Responses'][self.state_table_name][0]
        return response, entry

    def _get_output_table_response_and_entry(self):
        response = self.dynamo.batch_get_item(
            RequestItems={
                self.output_table_name: {
                    'Keys': [{'RequestId': self.request_id}]
                }
            }
        )
        entry = response['Responses'][self.output_table_name][0]
        return response, entry

    @mock.patch("matrix.common.date.get_datetime_now")
    def test_create_state_table_entry(self, mock_get_datetime_now):
        stub_date = '2019-03-18T180907.136216Z'
        mock_get_datetime_now.return_value = stub_date
        self.handler.create_state_table_entry(self.request_id)
        response, entry = self._get_state_table_response_and_entry()

        self.assertEqual(len(response['Responses'][self.state_table_name]), 1)

        self.assertTrue(all(field.value in entry for field in StateTableField))
        self.assertEqual(entry[StateTableField.EXPECTED_DRIVER_EXECUTIONS.value], 1)
        self.assertEqual(entry[StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value], 1)
        self.assertEqual(entry[StateTableField.CREATION_DATE.value], stub_date)

    def test_create_output_table_entry(self):
        self.handler.create_output_table_entry(self.request_id, 1, self.format)
        response, entry = self._get_output_table_response_and_entry()

        self.assertEqual(len(response['Responses'][self.output_table_name]), 1)

        self.assertTrue(all(field.value in entry for field in OutputTableField))
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

    def test_increment_table_field_state_table_path(self):
        self.handler.create_state_table_entry(self.request_id)

        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_DRIVER_EXECUTIONS.value], 0)
        self.assertEqual(entry[StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value], 0)

        field_enum = StateTableField.COMPLETED_DRIVER_EXECUTIONS
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_enum, 5)
        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_DRIVER_EXECUTIONS.value], 5)
        self.assertEqual(entry[StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value], 0)

    def test_increment_table_field_output_table_path(self):
        self.handler.create_output_table_entry(self.request_id, 1, self.format)

        response, entry = self._get_output_table_response_and_entry()
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

        field_enum = OutputTableField.ROW_COUNT
        self.handler.increment_table_field(DynamoTable.OUTPUT_TABLE, self.request_id, field_enum, 5)
        response, entry = self._get_output_table_response_and_entry()
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 5)

    def test_increment_field(self):
        self.handler.create_state_table_entry(self.request_id)
        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_DRIVER_EXECUTIONS.value], 0)
        self.assertEqual(entry[StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value], 0)

        key_dict = {"RequestId": self.request_id}
        field_enum = StateTableField.COMPLETED_DRIVER_EXECUTIONS
        self.handler._increment_field(self.handler._state_table, key_dict, field_enum, 15)
        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_DRIVER_EXECUTIONS.value], 15)
        self.assertEqual(entry[StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value], 0)

    def test_get_state_table_entry(self):
        self.handler.create_state_table_entry(self.request_id)
        entry = self.handler.get_table_item(DynamoTable.STATE_TABLE, request_id=self.request_id)
        self.assertEqual(entry[StateTableField.EXPECTED_DRIVER_EXECUTIONS.value], 1)

        key_dict = {"RequestId": self.request_id}
        field_enum = StateTableField.COMPLETED_DRIVER_EXECUTIONS
        self.handler._increment_field(self.handler._state_table, key_dict, field_enum, 15)
        entry = self.handler.get_table_item(DynamoTable.STATE_TABLE, request_id=self.request_id)
        self.assertEqual(entry[StateTableField.COMPLETED_DRIVER_EXECUTIONS.value], 15)

    def test_get_output_table_entry(self):
        self.handler.create_output_table_entry(self.request_id, 1, self.format)
        entry = self.handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_id=self.request_id)
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

        self.handler.increment_table_field(DynamoTable.OUTPUT_TABLE, self.request_id, OutputTableField.ROW_COUNT, 5)
        entry = self.handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_id=self.request_id)
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 5)

    def test_get_table_item(self):
        self.assertRaises(MatrixException, self.handler.get_table_item,
                          DynamoTable.OUTPUT_TABLE,
                          request_id=self.request_id)

        self.handler.create_output_table_entry(self.request_id, 1, self.format)
        entry = self.handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_id=self.request_id)
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

    def test_write_request_error(self):
        self.handler.create_output_table_entry(self.request_id, 1, self.format)

        self.handler.write_request_error(self.request_id, "test error")
        output = self.handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_id=self.request_id)
        self.assertEqual(output[OutputTableField.ERROR_MESSAGE.value], "test error")

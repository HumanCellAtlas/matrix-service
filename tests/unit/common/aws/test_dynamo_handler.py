import hashlib
import os
import uuid

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
        self.request_hash = hashlib.sha256().hexdigest()
        self.format = "zarr"

        self.create_test_state_table()
        self.create_test_output_table()
        self.create_test_cache_table()

        self.handler = DynamoHandler()

    def _get_state_table_response_and_entry(self):
        response = self.dynamo.batch_get_item(
            RequestItems={
                self.state_table_name: {
                    'Keys': [{'RequestHash': self.request_hash}]
                }
            }
        )
        entry = response['Responses'][self.state_table_name][0]
        return response, entry

    def _get_output_table_response_and_entry(self):
        response = self.dynamo.batch_get_item(
            RequestItems={
                self.output_table_name: {
                    'Keys': [{'RequestHash': self.request_hash}]
                }
            }
        )
        entry = response['Responses'][self.output_table_name][0]
        return response, entry

    def test_create_state_table_entry(self):
        num_mappers = 0
        self.handler.create_state_table_entry(self.request_hash, num_mappers)
        response, entry = self._get_state_table_response_and_entry()

        self.assertEqual(len(response['Responses'][self.state_table_name]), 1)

        self.assertTrue(all(field.value in entry for field in StateTableField))
        self.assertEqual(entry[StateTableField.EXPECTED_MAPPER_EXECUTIONS.value], num_mappers)
        self.assertEqual(entry[StateTableField.EXPECTED_WORKER_EXECUTIONS.value], 0)
        self.assertEqual(entry[StateTableField.EXPECTED_REDUCER_EXECUTIONS.value], 1)

    def test_create_output_table_entry(self):
        self.handler.create_output_table_entry(self.request_hash, 1, self.format)
        response, entry = self._get_output_table_response_and_entry()

        self.assertEqual(len(response['Responses'][self.output_table_name]), 1)

        self.assertTrue(all(field.value in entry for field in OutputTableField))
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

    def test_increment_table_field_state_table_path(self):
        self.handler.create_state_table_entry(self.request_hash, 1)

        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_WORKER_EXECUTIONS.value], 0)
        self.assertEqual(entry[StateTableField.COMPLETED_MAPPER_EXECUTIONS.value], 0)

        field_enum = StateTableField.COMPLETED_WORKER_EXECUTIONS
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_hash, field_enum, 5)
        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_WORKER_EXECUTIONS.value], 5)
        self.assertEqual(entry[StateTableField.COMPLETED_MAPPER_EXECUTIONS.value], 0)

    def test_increment_table_field_output_table_path(self):
        self.handler.create_output_table_entry(self.request_hash, 1, self.format)

        response, entry = self._get_output_table_response_and_entry()
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

        field_enum = OutputTableField.ROW_COUNT
        self.handler.increment_table_field(DynamoTable.OUTPUT_TABLE, self.request_hash, field_enum, 5)
        response, entry = self._get_output_table_response_and_entry()
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 5)

    def test_increment_field(self):
        self.handler.create_state_table_entry(self.request_hash, 1)
        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_WORKER_EXECUTIONS.value], 0)
        self.assertEqual(entry[StateTableField.COMPLETED_MAPPER_EXECUTIONS.value], 0)

        key_dict = {"RequestHash": self.request_hash}
        field_enum = StateTableField.COMPLETED_WORKER_EXECUTIONS
        self.handler._increment_field(self.handler._state_table, key_dict, field_enum, 15)
        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_WORKER_EXECUTIONS.value], 15)
        self.assertEqual(entry[StateTableField.COMPLETED_MAPPER_EXECUTIONS.value], 0)

    def test_get_state_table_entry(self):
        self.handler.create_state_table_entry(self.request_hash, 1)
        entry = self.handler.get_table_item(DynamoTable.STATE_TABLE, request_hash=self.request_hash)
        self.assertEqual(entry[StateTableField.EXPECTED_REDUCER_EXECUTIONS.value], 1)

        key_dict = {"RequestHash": self.request_hash}
        field_enum = StateTableField.COMPLETED_WORKER_EXECUTIONS
        self.handler._increment_field(self.handler._state_table, key_dict, field_enum, 15)
        entry = self.handler.get_table_item(DynamoTable.STATE_TABLE, request_hash=self.request_hash)
        self.assertEqual(entry[StateTableField.COMPLETED_WORKER_EXECUTIONS.value], 15)

    def test_get_output_table_entry(self):
        self.handler.create_output_table_entry(self.request_hash, 1, self.format)
        entry = self.handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_hash=self.request_hash)
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

        self.handler.increment_table_field(DynamoTable.OUTPUT_TABLE, self.request_hash, OutputTableField.ROW_COUNT, 5)
        entry = self.handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_hash=self.request_hash)
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 5)

    def test_get_table_item(self):
        self.assertRaises(ValueError, self.handler.get_table_item,
                          DynamoTable.OUTPUT_TABLE,
                          request_id=self.request_id,
                          request_hash=self.request_hash)

        self.assertRaises(ValueError, self.handler.get_table_item,
                          DynamoTable.OUTPUT_TABLE,
                          request_id=self.request_id)

        self.assertRaises(ValueError, self.handler.get_table_item,
                          DynamoTable.CACHE_TABLE,
                          request_hash=self.request_hash)

        self.assertRaises(MatrixException, self.handler.get_table_item,
                          DynamoTable.OUTPUT_TABLE,
                          request_hash=self.request_hash)

        self.handler.create_output_table_entry(self.request_hash, 1, self.format)
        entry = self.handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_hash=self.request_hash)
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

    def test_write_request_error(self):
        self.handler.create_output_table_entry(self.request_hash, 1, self.format)

        self.handler.write_request_error(self.request_hash, "test error")
        output = self.handler.get_table_item(DynamoTable.OUTPUT_TABLE, request_hash=self.request_hash)
        self.assertEqual(output[OutputTableField.ERROR_MESSAGE.value], "test error")

    def test_request_cache(self):

        self.assertIsNone(self.handler.get_request_hash(self.request_id))

        self.handler.write_request_hash(self.request_id, self.request_hash)
        self.assertEqual(self.handler.get_request_hash(self.request_id), self.request_hash)

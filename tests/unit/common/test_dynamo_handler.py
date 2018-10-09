import os
import uuid

import boto3

from .. import MatrixTestCaseUsingMockAWS
from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import StateTableField
from matrix.common.dynamo_handler import OutputTableField
from matrix.common.dynamo_handler import DynamoTable


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

        self.create_test_state_table(self.dynamo)
        self.create_test_output_table(self.dynamo)

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

    def test_create_state_table_entry(self):
        num_bundles = 2

        self.handler.create_state_table_entry(self.request_id, num_bundles)
        response, entry = self._get_state_table_response_and_entry()

        self.assertEqual(len(response['Responses'][self.state_table_name]), 1)

        self.assertTrue(all(field.value in entry for field in StateTableField))
        self.assertEqual(entry[StateTableField.EXPECTED_MAPPER_EXECUTIONS.value], num_bundles)
        self.assertEqual(entry[StateTableField.EXPECTED_REDUCER_EXECUTIONS.value], 1)

    def test_create_output_table_entry(self):
        self.handler.create_output_table_entry(self.request_id)
        response, entry = self._get_output_table_response_and_entry()

        self.assertEqual(len(response['Responses'][self.output_table_name]), 1)

        self.assertTrue(all(field.value in entry for field in OutputTableField))
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

    def test_increment_table_field_state_table_path(self):
        self.handler.create_state_table_entry(self.request_id, num_bundles=1)

        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_WORKER_EXECUTIONS.value], 0)
        self.assertEqual(entry[StateTableField.COMPLETED_MAPPER_EXECUTIONS.value], 0)

        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, StateTableField.COMPLETED_WORKER_EXECUTIONS.value, 5)
        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_WORKER_EXECUTIONS.value], 5)
        self.assertEqual(entry[StateTableField.COMPLETED_MAPPER_EXECUTIONS.value], 0)

    def test_increment_table_field_output_table_path(self):
        self.handler.create_output_table_entry(self.request_id)

        response, entry = self._get_output_table_response_and_entry()
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

        self.handler.increment_table_field(DynamoTable.OUTPUT_TABLE, self.request_id, OutputTableField.ROW_COUNT.value, 5)
        response, entry = self._get_output_table_response_and_entry()
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 5)

    def test_increment_field(self):
        self.handler.create_state_table_entry(self.request_id, num_bundles=1)
        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_WORKER_EXECUTIONS.value], 0)
        self.assertEqual(entry[StateTableField.COMPLETED_MAPPER_EXECUTIONS.value], 0)

        key_dict = {"RequestId": self.request_id}
        self.handler._increment_field(self.handler._state_table, key_dict, StateTableField.COMPLETED_WORKER_EXECUTIONS.value, 15)
        response, entry = self._get_state_table_response_and_entry()
        self.assertEqual(entry[StateTableField.COMPLETED_WORKER_EXECUTIONS.value], 15)
        self.assertEqual(entry[StateTableField.COMPLETED_MAPPER_EXECUTIONS.value], 0)

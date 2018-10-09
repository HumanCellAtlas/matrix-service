import os

import boto3

from .. import MatrixTestCaseUsingMockAWS
from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import StateTableField
from matrix.common.dynamo_handler import OutputTableField


class TestDynamoHandler(MatrixTestCaseUsingMockAWS):
    """
    Environment variables are set in tests/unit/__init__.py
    """
    def setUp(self):
        super(TestDynamoHandler, self).setUp()

        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self.state_table_name = os.environ['DYNAMO_STATE_TABLE_NAME']
        self.output_table_name = os.environ['DYNAMO_OUTPUT_TABLE_NAME']

        self.create_test_state_table(self.dynamo)
        self.create_test_output_table(self.dynamo)

        self.handler = DynamoHandler()

    def test_create_state_table_entry(self):
        request_id = "test_id"
        num_bundles = 2

        self.handler.create_state_table_entry(request_id, num_bundles)
        response = self.dynamo.batch_get_item(
            RequestItems={
                self.state_table_name: {
                    'Keys': [{'RequestId': request_id}]
                }
            }
        )

        self.assertEqual(len(response['Responses'][self.state_table_name]), 1)
        entry = response['Responses'][self.state_table_name][0]

        self.assertTrue(all(field.value in entry for field in StateTableField))
        self.assertEqual(entry[StateTableField.EXPECTED_MAPPER_EXECUTIONS.value], num_bundles)
        self.assertEqual(entry[StateTableField.EXPECTED_REDUCER_EXECUTIONS.value], 1)

    def test_create_output_table_entry(self):
        request_id = "test_id"

        self.handler.create_output_table_entry(request_id)
        response = self.dynamo.batch_get_item(
            RequestItems={
                self.output_table_name: {
                    'Keys': [{'RequestId': request_id}]
                }
            }
        )

        self.assertEqual(len(response['Responses'][self.output_table_name]), 1)
        entry = response['Responses'][self.output_table_name][0]

        self.assertTrue(all(field.value in entry for field in OutputTableField))
        self.assertEqual(entry[OutputTableField.ROW_COUNT.value], 0)

import os

import boto3

from .. import MatrixTestCaseUsingMockAWS
from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import StateTableField


class TestDynamoHandler(MatrixTestCaseUsingMockAWS):
    """
    Environment variables are set in tests/unit/__init__.py
    """
    def setUp(self):
        super(TestDynamoHandler, self).setUp()

        self.handler = DynamoHandler()
        self.state_table_name = os.environ['DYNAMO_STATE_TABLE_NAME']
        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])

        self._create_test_state_table()

    def test_put_state_item(self):
        request_id = "test_id"
        num_bundles = 2

        self.handler.put_state_item(request_id, num_bundles)
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

    def _create_test_state_table(self):
        self.dynamo.create_table(
            TableName=self.state_table_name,
            KeySchema=[
                {
                    'AttributeName': "RequestId",
                    'KeyType': "HASH",
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': "RequestId",
                    'AttributeType': "S",
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 25,
                'WriteCapacityUnits': 25,
            },
        )

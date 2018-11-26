import os
import unittest

import boto3

from moto import mock_dynamodb2, mock_s3

os.environ['ACCOUNT_ID'] = "test_account_id"
os.environ['DEPLOYMENT_STAGE'] = "dev"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"
os.environ['AWS_ACCESS_KEY_ID'] = "ak"
os.environ['AWS_SECRET_ACCESS_KEY'] = "sk"
os.environ['LAMBDA_DRIVER_FUNCTION_NAME'] = f"dcp-matrix-service-driver-{os.environ['DEPLOYMENT_STAGE']}"
os.environ['DYNAMO_STATE_TABLE_NAME'] = f"dcp-matrix-service-state-table-{os.environ['DEPLOYMENT_STAGE']}"
os.environ['DYNAMO_OUTPUT_TABLE_NAME'] = f"dcp-matrix-service-output-table-{os.environ['DEPLOYMENT_STAGE']}"
os.environ['DYNAMO_CACHE_TABLE_NAME'] = f"dcp-matrix-service-cache-table-{os.environ['DEPLOYMENT_STAGE']}"
os.environ['S3_RESULTS_BUCKET'] = f"dcp-matrix-service-results-{os.environ['DEPLOYMENT_STAGE']}"
os.environ['DYNAMO_LOCK_TABLE_NAME'] = f"dcp-matrix-service-lock-table-{os.environ['DEPLOYMENT_STAGE']}"


class MatrixTestCaseUsingMockAWS(unittest.TestCase):
    def setUp(self):
        self.dynamo_mock = mock_dynamodb2()
        self.dynamo_mock.start()
        self.s3_mock = mock_s3()
        self.s3_mock.start()

    def tearDown(self):
        self.dynamo_mock.stop()
        self.s3_mock.stop()

    @staticmethod
    def create_test_state_table():
        boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION']).create_table(
            TableName=os.environ['DYNAMO_STATE_TABLE_NAME'],
            KeySchema=[
                {
                    'AttributeName': "RequestHash",
                    'KeyType': "HASH",
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': "RequestHash",
                    'AttributeType': "S",
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 25,
                'WriteCapacityUnits': 25,
            },
        )

    @staticmethod
    def create_test_output_table():
        boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION']).create_table(
            TableName=os.environ['DYNAMO_OUTPUT_TABLE_NAME'],
            KeySchema=[
                {
                    'AttributeName': "RequestHash",
                    'KeyType': "HASH",
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': "RequestHash",
                    'AttributeType': "S",
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 15,
                'WriteCapacityUnits': 15,
            },
        )

    @staticmethod
    def create_test_cache_table():
        boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION']).create_table(
            TableName=os.environ['DYNAMO_CACHE_TABLE_NAME'],
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
                'ReadCapacityUnits': 15,
                'WriteCapacityUnits': 15,
            },
        )

    @staticmethod
    def create_s3_results_bucket():
        boto3.resource("s3", region_name=os.environ['AWS_DEFAULT_REGION']) \
             .create_bucket(Bucket=os.environ['S3_RESULTS_BUCKET'])

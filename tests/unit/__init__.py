import os
import unittest

import boto3

from moto import mock_dynamodb2, mock_s3

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
os.environ['BATCH_CONVERTER_JOB_QUEUE_ARN'] = "test-job-queue"
os.environ['BATCH_CONVERTER_JOB_DEFINITION_ARN'] = "test-job-definition"

test_bundle_spec = {
    "uuid": "ba9c63ac-6db5-48bc-a2e3-7be4ddd03d97",
    "version": "2018-10-17T173508.111787Z",
    "replica": "aws",
    "description": {
        "shapes": {
            "cell_id": (1,),
            "cell_metadata_numeric": (1, 151),
            "cell_metadata_numeric_name": (151,),
            "cell_metadata_string": (1, 3),
            "cell_metadata_string_name": (3,),
            "expression": (1, 58347),
            "gene_id": (58347,)
        },
        "sums": {
            "expression": 1000000,
            "cell_metadata_numeric": 5859988
        },
        "digests": {
            "cell_id": b'fefa87f9820656e61298827f94fc537c198b8b23',
            "cell_metadata_numeric": b'49181b9040b3cfa386cb2282c00a25eddbf480d5',
            "cell_metadata_numeric_name": b'f4e8156b933c1295afd5468c398991cd9712ba26',
            "cell_metadata_string": b'ef70f6bde79d7f61cf6eb89e16b29bec912565fb',
            "cell_metadata_string_name": b'bdd2ea7e0b4424872681f778c78cc8f0ee66cfa2',
            "expression": b'b8b66e56cdff65c719eea8b9313e990ec01237b3',
            "gene_id": b'c251ac69ceda370d512f37873715114c25fdfb39'
        }
    }
}


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

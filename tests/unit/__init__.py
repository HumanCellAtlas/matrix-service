import os
import unittest

import boto3
from moto import mock_dynamodb2, mock_s3, mock_sqs, mock_sts

from matrix.common.config import MatrixInfraConfig, MatrixRedshiftConfig

os.environ['DEPLOYMENT_STAGE'] = "test_deployment_stage"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"
os.environ['AWS_ACCESS_KEY_ID'] = "test_ak"
os.environ['AWS_SECRET_ACCESS_KEY'] = "test_sk"
os.environ['LAMBDA_DRIVER_FUNCTION_NAME'] = "test_driver_name"
os.environ['LAMBDA_NOTIFICATIONS_FUNCTION_NAME'] = "test_notifications_name"
os.environ['DYNAMO_STATE_TABLE_NAME'] = "test_state_table_name"
os.environ['DYNAMO_OUTPUT_TABLE_NAME'] = "test_output_table_name"
os.environ['MATRIX_RESULTS_BUCKET'] = "test_results_bucket"
os.environ['MATRIX_QUERY_BUCKET'] = "test_query_bucket"
os.environ['MATRIX_PRELOAD_BUCKET'] = "test_preload_bucket"
os.environ['MATRIX_REDSHIFT_IAM_ROLE_ARN'] = "test_redshift_role"
os.environ['BATCH_CONVERTER_JOB_QUEUE_ARN'] = "test-job-queue"
os.environ['BATCH_CONVERTER_JOB_DEFINITION_ARN'] = "test-job-definition"


class MatrixTestCaseUsingMockAWS(unittest.TestCase):

    TEST_CONFIG = {
        'query_job_q_url': 'test_query_job_q_name',
        'query_job_deadletter_q_url': 'test_deadletter_query_job_q_name'
    }
    TEST_REDSHIFT_CONFIG = {
        'database_uri': 'test_database_uri',
        'redshift_role_arn': 'test_redshift_role_arn'
    }

    def setUp(self):
        self.dynamo_mock = mock_dynamodb2()
        self.dynamo_mock.start()
        self.s3_mock = mock_s3()
        self.s3_mock.start()
        self.sqs_mock = mock_sqs()
        self.sqs_mock.start()
        self.sts_mock = mock_sts()
        self.sts_mock.start()

        self.matrix_infra_config = MatrixInfraConfig()
        self.redshift_config = MatrixRedshiftConfig()

        self.sqs = boto3.resource('sqs')
        self.sqs.create_queue(QueueName=f"test_query_job_q_name")
        self.sqs.create_queue(QueueName=f"test_deadletter_query_job_q_name")

    def tearDown(self):
        self.dynamo_mock.stop()
        self.s3_mock.stop()

    @staticmethod
    def create_test_state_table():
        boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION']).create_table(
            TableName=os.environ['DYNAMO_STATE_TABLE_NAME'],
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

    @staticmethod
    def create_test_output_table():
        boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION']).create_table(
            TableName=os.environ['DYNAMO_OUTPUT_TABLE_NAME'],
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

    @staticmethod
    def create_s3_queries_bucket():
        boto3.resource("s3", region_name=os.environ['AWS_DEFAULT_REGION']) \
             .create_bucket(Bucket=os.environ['MATRIX_QUERY_BUCKET'])

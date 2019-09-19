import os
import unittest

import boto3
from moto import mock_dynamodb2, mock_s3, mock_sqs, mock_sts, mock_secretsmanager

os.environ['DEPLOYMENT_STAGE'] = "test_deployment_stage"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"
os.environ['AWS_ACCESS_KEY_ID'] = "test_ak"
os.environ['AWS_SECRET_ACCESS_KEY'] = "test_sk"
os.environ['LAMBDA_DRIVER_V0_FUNCTION_NAME'] = "test_driver_v0_name"
os.environ['LAMBDA_DRIVER_V1_FUNCTION_NAME'] = "test_driver_v1_name"
os.environ['LAMBDA_NOTIFICATION_FUNCTION_NAME'] = "test_notification_name"
os.environ['DYNAMO_DATA_VERSION_TABLE_NAME'] = "test_data_version_table_name"
os.environ['DYNAMO_DEPLOYMENT_TABLE_NAME'] = "test_deployment_table_name"
os.environ['DYNAMO_REQUEST_TABLE_NAME'] = "test_request_table_name"
os.environ['MATRIX_RESULTS_BUCKET'] = "test_results_bucket"
os.environ['MATRIX_QUERY_BUCKET'] = "test_query_bucket"
os.environ['MATRIX_QUERY_RESULTS_BUCKET'] = "test_query_results_bucket"
os.environ['MATRIX_PRELOAD_BUCKET'] = "test_preload_bucket"
os.environ['MATRIX_REDSHIFT_IAM_ROLE_ARN'] = "test_redshift_role"
os.environ['BATCH_CONVERTER_JOB_QUEUE_ARN'] = "test-job-queue"
os.environ['BATCH_CONVERTER_JOB_DEFINITION_ARN'] = "test-job-definition"

# must be imported after test environment variables are set
from matrix.common.aws.dynamo_handler import DataVersionTableField, DeploymentTableField  # noqa
from matrix.common.config import MatrixInfraConfig, MatrixRedshiftConfig  # noqa


class MatrixTestCaseUsingMockAWS(unittest.TestCase):

    TEST_CONFIG = {
        'query_job_q_url': 'test_query_job_q_name',
        'query_job_deadletter_q_url': 'test_deadletter_query_job_q_name',
        'notification_q_url': 'test_notification_q_url'
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
        self.secrets_mock = mock_secretsmanager()
        self.secrets_mock.start()
        self.sqs_mock = mock_sqs()
        self.sqs_mock.start()
        self.sts_mock = mock_sts()
        self.sts_mock.start()

        self.matrix_infra_config = MatrixInfraConfig()
        self.redshift_config = MatrixRedshiftConfig()

        self.sqs = boto3.resource('sqs')
        self.sqs.create_queue(QueueName=f"test_query_job_q_name")
        self.sqs.create_queue(QueueName=f"test_deadletter_query_job_q_name")
        self.sqs.create_queue(QueueName=f"test_notification_q_url")

    def tearDown(self):
        self.dynamo_mock.stop()
        self.s3_mock.stop()

    @staticmethod
    def create_test_data_version_table():
        boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION']).create_table(
            TableName=os.environ['DYNAMO_DATA_VERSION_TABLE_NAME'],
            KeySchema=[
                {
                    'AttributeName': "DataVersion",
                    'KeyType': "HASH",
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': "DataVersion",
                    'AttributeType': "S",
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 25,
                'WriteCapacityUnits': 25,
            },
        )

    @staticmethod
    def create_test_deployment_table():
        boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION']).create_table(
            TableName=os.environ['DYNAMO_DEPLOYMENT_TABLE_NAME'],
            KeySchema=[
                {
                    'AttributeName': "Deployment",
                    'KeyType': "HASH",
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': "Deployment",
                    'AttributeType': "S",
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 25,
                'WriteCapacityUnits': 25,
            },
        )

    @staticmethod
    def create_test_request_table():
        boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION']).create_table(
            TableName=os.environ['DYNAMO_REQUEST_TABLE_NAME'],
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
    def init_test_data_version_table():
        dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        data_version_table = dynamo.Table(os.environ['DYNAMO_DATA_VERSION_TABLE_NAME'])
        data_version_table.put_item(
            Item={
                DataVersionTableField.DATA_VERSION.value: 0,
                DataVersionTableField.CREATION_DATE.value: "test_date",
                DataVersionTableField.PROJECT_CELL_COUNTS.value: {'test_project': 1},
                DataVersionTableField.METADATA_SCHEMA_VERSIONS.value: {},
            }
        )

    @staticmethod
    def init_test_deployment_table():
        dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        deployment_table = dynamo.Table(os.environ['DYNAMO_DEPLOYMENT_TABLE_NAME'])
        deployment_table.put_item(
            Item={
                DeploymentTableField.DEPLOYMENT.value: os.environ['DEPLOYMENT_STAGE'],
                DeploymentTableField.CURRENT_DATA_VERSION.value: 0
            }
        )

    @staticmethod
    def create_s3_results_bucket():
        boto3.resource("s3", region_name=os.environ['AWS_DEFAULT_REGION']) \
             .create_bucket(Bucket=os.environ['MATRIX_RESULTS_BUCKET'])

    @staticmethod
    def create_s3_queries_bucket():
        boto3.resource("s3", region_name=os.environ['AWS_DEFAULT_REGION']) \
             .create_bucket(Bucket=os.environ['MATRIX_QUERY_BUCKET'])

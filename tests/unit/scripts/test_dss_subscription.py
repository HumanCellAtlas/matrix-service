import json
import mock
import os

import boto3

from matrix.common import constants
from matrix.common.etl import get_dss_client
from scripts.dss_subscription import (recreate_dss_subscription,
                                      _generate_metadata_schema_version_clause,
                                      _regenerate_and_set_hmac_secret_key,
                                      DSS_SUBSCRIPTION_HMAC_SECRET_ID)
from tests.unit import MatrixTestCaseUsingMockAWS


DSS_CLIENT = get_dss_client("dev")


class TestDssSubscription(MatrixTestCaseUsingMockAWS):
    def setUp(self):
        super(TestDssSubscription, self).setUp()
        self.swagger_spec_stub = {
            'info': {
                'description': "test_description"
            },
            'host': "test_host",
            'basePath': "/v1",
            'paths': {}
        }

    @mock.patch("secrets.token_hex")
    def test_regenerate_and_set_hmac_secret(self, mock_token_hex):
        secret_name = f"dcp/matrix/{os.environ['DEPLOYMENT_STAGE']}/infra"
        test_hmac_secret_key = "test_hmac_secret_key"
        mock_token_hex.return_value = test_hmac_secret_key
        test_secret = {
            DSS_SUBSCRIPTION_HMAC_SECRET_ID: test_hmac_secret_key
        }

        secrets_client = boto3.client("secretsmanager", region_name=os.environ['AWS_DEFAULT_REGION'])
        secrets_client.create_secret(Name=secret_name,
                                     SecretString="{}")

        _regenerate_and_set_hmac_secret_key()

        secret = secrets_client.get_secret_value(SecretId=secret_name)
        decoded = json.loads(secret['SecretString'])
        self.assertEqual(decoded, test_secret)

    @mock.patch("hca.dss.DSSClient.put_subscription")
    @mock.patch("hca.dss.DSSClient.delete_subscription")
    @mock.patch("hca.dss.DSSClient.get_subscriptions")
    @mock.patch("scripts.dss_subscription._regenerate_and_set_hmac_secret_key")
    @mock.patch("scripts.dss_subscription._generate_metadata_schema_version_clause")
    @mock.patch("hca.dss.DSSClient.swagger_spec", new_callable=mock.PropertyMock)
    @mock.patch("scripts.dss_subscription.retrieve_gcp_credentials")
    def test_recreate_dss_subscription(self,
                                       mock_retrieve_gcp_credentials,
                                       mock_swagger_spec,
                                       mock_generate_metadata_schema_version_clause,
                                       mock_regenerate_and_set_hmac_secret_key,
                                       mock_get_subscriptions,
                                       mock_delete_subscription,
                                       mock_put_subscription):
        callback = f"https://matrix.{os.environ['DEPLOYMENT_STAGE']}.data.humancellatlas.org/dss/notification"
        mock_retrieve_gcp_credentials.return_value = {}
        mock_swagger_spec.return_value = self.swagger_spec_stub
        mock_get_subscriptions.return_value = {'subscriptions': [{'uuid': "test_uuid", 'callback_url': callback}]}
        mock_regenerate_and_set_hmac_secret_key.return_value = "test_hmac_secret_key"

        recreate_dss_subscription()

        mock_get_subscriptions.assert_called_once_with(replica="aws",
                                                       subscription_type="jmespath")
        mock_delete_subscription.assert_called_once_with(replica="aws",
                                                         subscription_type="jmespath",
                                                         uuid="test_uuid")
        mock_put_subscription.assert_called_once_with(callback_url=callback,
                                                      jmespath_query=mock.ANY,
                                                      replica="aws",
                                                      hmac_key_id=DSS_SUBSCRIPTION_HMAC_SECRET_ID,
                                                      hmac_secret_key="test_hmac_secret_key")
        # self.assertEqual(mock_generate_metadata_schema_version_clause.call_count,
        #                  len(constants.SUPPORTED_METADATA_SCHEMA_VERSIONS))

    def test_generate_metadata_schema_version_clause(self):
        project_md_schema_versions = constants.SUPPORTED_METADATA_SCHEMA_VERSIONS[constants.MetadataSchemaName.PROJECT]
        constants.SUPPORTED_METADATA_SCHEMA_VERSIONS[constants.MetadataSchemaName.PROJECT] = {
            'max_major': 56,
            'max_minor': 78,
            'min_major': 12,
            'min_minor': 34
        }

        metadata_schema_version_clause = _generate_metadata_schema_version_clause(constants.MetadataSchemaName.PROJECT)

        expected = (
            "((files.project_json[].provenance.schema_major_version==`12` && "
            "files.project_json[].provenance.schema_minor_version>=`34`) || "
            "(files.project_json[].provenance.schema_major_version==`56` && "
            "files.project_json[].provenance.schema_minor_version<=`78`) || "
            "(files.project_json[].provenance.schema_major_version<`56` && "
            "files.project_json[].provenance.schema_major_version>`12`) || "
            "(files.project_json[].provenance.schema_major_version==`null`))"
        )

        print(metadata_schema_version_clause)
        self.assertEqual(metadata_schema_version_clause, expected)
        constants.SUPPORTED_METADATA_SCHEMA_VERSIONS[constants.MetadataSchemaName.PROJECT] = project_md_schema_versions

    class MatrixInfraConfigStub:
        def __init__(self):
            self.gcp_service_acct_creds = "{}".encode()

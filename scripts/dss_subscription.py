import base64
import json
import os
import secrets
import sys
import traceback

import boto3
import hca

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from matrix.common.config import MatrixInfraConfig
from matrix.common.constants import SUPPORTED_METADATA_SCHEMA_VERSIONS, MetadataSchemaName


DSS_SUBSCRIPTION_HMAC_SECRET_ID = "dss_subscription_hmac_secret_key"


def retrieve_gcp_credentials():  # pragma: no cover
    return json.loads(base64.b64decode(MatrixInfraConfig().gcp_service_acct_creds).decode())


def _generate_metadata_schema_version_clause(schema: MetadataSchemaName):
    supported_versions = SUPPORTED_METADATA_SCHEMA_VERSIONS[schema]
    min_major_case = (
        f"(files.{schema.value}_json[].provenance.schema_major_version==`{supported_versions['min_major']}` && "
        f"files.{schema.value}_json[].provenance.schema_minor_version>=`{supported_versions['min_minor']}`)"
    )

    max_major_case = (
        f"(files.{schema.value}_json[].provenance.schema_major_version==`{supported_versions['max_major']}` && "
        f"files.{schema.value}_json[].provenance.schema_minor_version<=`{supported_versions['max_minor']}`)"
    )

    in_between_case = (
        f"(files.{schema.value}_json[].provenance.schema_major_version<`{supported_versions['max_major']}` && "
        f"files.{schema.value}_json[].provenance.schema_major_version>`{supported_versions['min_major']}`)"
    )

    null_case = f"(files.{schema.value}_json[].provenance.schema_major_version==`null`)"

    return f"({min_major_case} || {max_major_case} || {in_between_case} || {null_case})"


def _regenerate_and_set_hmac_secret_key():
    """
    Generates and stores in AWS SecretsManager an HMAC secret key used
    to sign and verify DSS notification events.
    :return: str Generated HMAC secret key.
    """
    secrets_client = boto3.client("secretsmanager", region_name=os.environ['AWS_DEFAULT_REGION'])

    deployment_stage = os.environ['DEPLOYMENT_STAGE']
    secret_id = f"dcp/matrix/{deployment_stage}/infra"
    # generate new key
    hmac_secret_key = secrets.token_hex(16)

    # set key
    secret = secrets_client.get_secret_value(SecretId=secret_id)
    blob = json.loads(secret['SecretString'])
    blob[DSS_SUBSCRIPTION_HMAC_SECRET_ID] = hmac_secret_key
    secrets_client.put_secret_value(SecretId=secret_id,
                                    SecretString=json.dumps(blob))

    return hmac_secret_key


def recreate_dss_subscription():
    deployment_stage = os.environ['DEPLOYMENT_STAGE']
    if deployment_stage == "prod":
        print("Quitting... DSS Subscriptions are not enabled in prod.")
        return

    gcp_service_acct_creds = retrieve_gcp_credentials()
    with open('gcp_creds.json', 'w') as outfile:
        json.dump(gcp_service_acct_creds, outfile)

    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + "/gcp_creds.json"
        replica = "aws"
        # In order to enable DCP integration tests, only subscribe to test bundles
        jmespath_query = (
            f"(event_type==`CREATE`)"
            f" && (files.analysis_process_json[0].type.text==`analysis`)"
            f" && (files.project_json[].project_core.project_short_name | starts_with(@[0], `{deployment_stage}/`))"
            f" && (files.library_preparation_protocol_json[0].library_construction_method.ontology==`EFO:0008931`"
            f" || files.library_preparation_protocol_json[0].library_construction_method.ontology==`EFO:0009310`)"
        )

        # Schema versions are not enforced while subscriptions only apply to test bundles
        # for schema in SUPPORTED_METADATA_SCHEMA_VERSIONS:
        #     jmespath_query += f" && {_generate_metadata_schema_version_clause(schema)}"

        if deployment_stage == "prod":
            swagger_url = "https://dss.data.humancellatlas.org/v1/swagger.json"
            matrix_callback = "https://matrix.data.humancellatlas.org/dss/notification"
        else:
            swagger_url = f"https://dss.{deployment_stage}.data.humancellatlas.org/v1/swagger.json"
            matrix_callback = f"https://matrix.{deployment_stage}.data.humancellatlas.org/dss/notification"

        dss_client = hca.dss.DSSClient(swagger_url=swagger_url)

        hmac_secret_key = _regenerate_and_set_hmac_secret_key()

        for subscription in dss_client.get_subscriptions(replica=replica, subscription_type="jmespath")['subscriptions']:
            if matrix_callback == subscription["callback_url"]:
                res = dss_client.delete_subscription(replica=replica,
                                                     subscription_type="jmespath",
                                                     uuid=subscription["uuid"])
                print("Deleted subscription {}: {}".format(subscription["uuid"], res))

        resp = dss_client.put_subscription(callback_url=matrix_callback,
                                           jmespath_query=jmespath_query,
                                           replica="aws",
                                           hmac_key_id=DSS_SUBSCRIPTION_HMAC_SECRET_ID,
                                           hmac_secret_key=hmac_secret_key)
        print("Created subscription {}: {}".format(resp["uuid"], resp))

    except Exception as e:
        print(e)
        traceback.print_exc()
    os.remove(os.getcwd() + "/gcp_creds.json")
    print("deleted creds file")


if __name__ == '__main__':  # pragma: no cover
    recreate_dss_subscription()

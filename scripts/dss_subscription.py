import base64
import json
import os
import requests

import hca
from matrix.common.config import MatrixInfraConfig


# DSSClient functions are generated at runtime and are difficult to mock
def get_subscriptions(dss_client: hca.dss.DSSClient,
                      replica: str,
                      subscription_type: str):  # pragma: no cover
    return dss_client.get_subscriptions(replica=replica,
                                        subscription_type=subscription_type)['subscriptions']


def delete_subscription(dss_client: hca.dss.DSSClient,
                        replica: str,
                        subscription_type: str,
                        uuid: str):  # pragma: no cover
    return dss_client.delete_subscription(replica=replica,
                                          subscription_type=subscription_type,
                                          uuid=uuid)


def put_subscription(dss_client: hca.dss.DSSClient,
                     callback_url: str,
                     jmespath_query: str,
                     replica: str):  # pragma: no cover
    return dss_client.put_subscription(callback_url=callback_url,
                                       jmespath_query=jmespath_query,
                                       replica=replica)


def retrieve_gcp_credentials():  # pragma: no cover
    return json.loads(base64.b64decode(MatrixInfraConfig().gcp_service_acct_creds).decode())


def recreate_dss_subscription():
    gcp_service_acct_creds = retrieve_gcp_credentials()
    with open('gcp_creds.json', 'w') as outfile:
        json.dump(gcp_service_acct_creds, outfile)
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + "/gcp_creds.json"
        deployment_stage = os.environ['DEPLOYMENT_STAGE']
        replica = "aws"
        jmespath_query = "(event_type==`CREATE` || event_type==`TOMBSTONE` || event_type==`DELETE`) \
            && (files.library_preparation_protocol[].library_construction_method.ontology==`EFO:0008931` \
            || files.library_preparation_protocol[].library_construction_method.ontology_label==`10X v2 sequencing`) \
            && files.analysis_process[].type.text==`analysis`"

        if deployment_stage == "prod":
            swagger_url = "https://dss.data.humancellatlas.org/v1/swagger.json"
            matrix_callback = "https://matrix.data.humancellatlas.org/v0/dss/notifications"
        else:
            swagger_url = f"https://dss.{deployment_stage}.data.humancellatlas.org/v1/swagger.json"
            matrix_callback = f"https://matrix.{deployment_stage}.data.humancellatlas.org/v0/dss/notifications"

        dss_client = hca.dss.DSSClient(swagger_url=swagger_url)

        for subscription in get_subscriptions(dss_client=dss_client, replica=replica, subscription_type="jmespath"):
            if matrix_callback == subscription["callback_url"]:
                res = delete_subscription(dss_client=dss_client,
                                          replica=replica,
                                          subscription_type="jmespath",
                                          uuid=subscription["uuid"])
                print("Deleted subscription {}: {}".format(subscription["uuid"], res))

        print("Not resubscribing to dss for the time being.")
        # resp = put_subscription(dss_client=dss_client,
        #                         callback_url=matrix_callback,
        #                         jmespath_query=jmespath_query,
        #                         replica="aws")
        # print("Created subscription {}: {}".format(resp["uuid"], resp))

    except Exception as e:
        print(e)
    os.remove(os.getcwd() + "/gcp_creds.json")
    print("deleted creds file")


if __name__ == '__main__':  # pragma: no cover
    recreate_dss_subscription()

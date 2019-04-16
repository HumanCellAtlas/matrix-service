import base64
import json
import os
import requests

import hca
from matrix.common.config import MatrixInfraConfig

if __name__ == '__main__':
    gcp_service_acct_creds = json.loads(base64.b64decode(MatrixInfraConfig().gcp_service_acct_creds).decode())
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

        for subscription in dss_client.get_subscriptions(replica=replica, subscription_type="jmespath")["subscriptions"]:
            if matrix_callback == subscription["callback_url"]:
                res = dss_client.delete_subscription(replica=replica, subscription_type="jmespath", uuid=subscription["uuid"])
                print("Deleted subscription {}: {}".format(subscription["uuid"], res))

        resp = dss_client.put_subscription(callback_url=matrix_callback,
                                           jmespath_query=jmespath_query,
                                           replica="aws")
        print("Created subscription {}: {}".format(resp["uuid"], resp))

    except Exception as e:
        print(e)
    print("deleted creds file")
    os.remove(os.getcwd() + "/gcp_creds.json")

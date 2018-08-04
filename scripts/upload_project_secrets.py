import json
import sys

from dcplib.aws_secret import AwsSecret

# Template for project configuration which will be stored in aws secret manager.
# Fill in the fields and run this script for uploading the config into secret manager.
CONFIG_TEMPLATE = {
  "aws_profile": "",
  "aws_region": "",
  "hca_ms_deployment_bucket": "",
  "hca_ms_merged_mtx_bucket": "",
  "ms_sqs_queue": "",
  "ms_dead_letter_queue": "",
  "ms_secret_name": "",
  "hca_host": "",
  "ms_dynamodb": ""
}

if __name__ == "__main__":
    value = json.dumps(CONFIG_TEMPLATE)
    secret = AwsSecret(name=sys.argv[1])
    secret.update(value=value)

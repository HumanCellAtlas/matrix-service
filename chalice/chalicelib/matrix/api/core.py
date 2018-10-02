import json
import os
import requests

import boto3

LAMBDA_CLIENT = boto3.client("lambda", region_name=os.environ['AWS_DEFAULT_REGION'])


def post_matrix(body):
    print(body)
    LAMBDA_CLIENT.invoke(
        FunctionName=os.environ['DRIVER_FN_NAME'],
        InvocationType="Event",
        Payload=json.dumps(body).encode(),
    )
    return "post_matrix", requests.codes.ok


def get_matrix(request_id):
    return {'request_id': request_id,
            'status': "In Progress",
            'key': "sample key",
            'eta': "sample eta",
            'message': "sample message",
            'links': [],
            }, requests.codes.ok

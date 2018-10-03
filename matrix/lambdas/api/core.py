import json
import os
import requests

import boto3

from ...common.constants import MatrixFormat

LAMBDA_CLIENT = boto3.client("lambda",
                             region_name=os.environ['AWS_DEFAULT_REGION'])


def post_matrix(body):
    has_ids = 'bundle_fqids' in body
    has_url = 'bundle_fqids_url' in body

    format = body['format'] if 'format' in body else MatrixFormat.ZARR.value

    # validate input parameters
    if not (has_ids or has_url):
        return {
                   'code': requests.codes.bad_request,
                   'message': "Missing required parameter. "
                              "One of `bundle_fqids` or `bundle_fqids_url` must be supplied. "
                              "Visit https://matrix.dev.data.humancellatlas.org for more information."
               }, requests.codes.bad_request

    if has_ids and has_url:
        return {
                   'code': requests.codes.bad_request,
                   'message': "Invalid parameters supplied. "
                              "Must supply either one of `bundle_fqids` or `bundle_fqids_url`. "
                              "Visit https://matrix.dev.data.humancellatlas.org for more information."
               }, requests.codes.bad_request

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

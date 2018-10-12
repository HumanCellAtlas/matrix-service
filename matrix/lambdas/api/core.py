import os
import requests
import uuid

from matrix.common.constants import MatrixFormat
from matrix.common.constants import MatrixRequestStatus
from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import DynamoTable
from matrix.common.dynamo_handler import StateTableField
from matrix.common.lambda_handler import LambdaHandler
from matrix.common.lambda_handler import LambdaName


def post_matrix(body: dict):
    has_ids = 'bundle_fqids' in body
    has_url = 'bundle_fqids_url' in body

    format = body['format'] if 'format' in body else MatrixFormat.ZARR.value

    # Validate input parameters
    if has_ids and has_url:
        return {
            'code': requests.codes.bad_request,
            'message': "Invalid parameters supplied. "
                       "Must supply either one of `bundle_fqids` or `bundle_fqids_url`. "
                       "Visit https://matrix.dev.data.humancellatlas.org for more information."
        }, requests.codes.bad_request

    if not has_ids and not has_url:
        return {
            'code': requests.codes.bad_request,
            'message': "Missing required parameter. "
                       "One of `bundle_fqids` or `bundle_fqids_url` must be supplied. "
                       "Visit https://matrix.dev.data.humancellatlas.org for more information."
        }, requests.codes.bad_request

    # TODO: test when URL param is supported
    if has_url:
        response = requests.get(body['bundle_fqids_url'])
        bundle_fqids = [b.strip() for b in response.text.split()]
    else:
        bundle_fqids = body['bundle_fqids']

    request_id = str(uuid.uuid4())
    driver_payload = {
        'request_id': request_id,
        'bundle_fqids': bundle_fqids,
        'format': format,
    }
    lambda_handler = LambdaHandler()
    lambda_handler.invoke(LambdaName.DRIVER, driver_payload)

    return {'request_id': request_id,
            'status': MatrixRequestStatus.IN_PROGRESS.value,
            'key': "",
            'eta': "",
            'message': "Job started.",
            'links': [],
            }, requests.codes.accepted


# TODO: write tests
def get_matrix(request_id: str):
    dynamo_handler = DynamoHandler()
    # TODO: error handling: invalid request_id
    ready, ready = dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                        request_id,
                                                        StateTableField.COMPLETED_REDUCER_EXECUTIONS.value,
                                                        0)
    if ready:
        # TODO: error handling: missing zarr (corrupted zarr?)
        s3_key = f"s3://{os.environ['S3_RESULTS_BUCKET']}/{request_id}.zarr"
        return {'request_id': request_id,
                'status': "Complete",
                'key': s3_key,
                'eta': "N/A",
                'message': f"Request {request_id} has successfully completed. The resultant expression matrix can be "
                           f"downloaded at the S3 location {s3_key}",
                'links': [],
                }, requests.codes.ok

    # TODO: return appropriate code if result is not ready
    return {'request_id': request_id,
            'status': "In progress",
            'key': "N/A",
            'eta': "N/A",
            'message': "Request {request_id} is still processing. Please try again later.",
            'links': [],
            }, requests.codes.ok

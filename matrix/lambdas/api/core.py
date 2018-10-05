import requests
import uuid

from ...common.constants import MatrixFormat
from ...common.constants import MatrixRequestStatus
from ...common.lambda_handler import LambdaHandler
from ...common.lambda_handler import LambdaName


def post_matrix(body):
    has_ids = 'bundle_fqids' in body
    has_url = 'bundle_fqids_url' in body

    format = body['format'] if 'format' in body else MatrixFormat.ZARR.value

    # Validate input parameters
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

    # TODO: read bundle ids from URL

    lambda_handler = LambdaHandler()
    if has_ids:
        request_id = str(uuid.uuid4())
        body.update({
            'request_id': request_id,
            'format': format,
        })
        lambda_handler.invoke(LambdaName.DRIVER, body)

        return {'request_id': request_id,
                'status': MatrixRequestStatus.IN_PROGRESS.value,
                'key': "",
                'eta': "",
                'message': "Job started.",
                'links': [],
                }, requests.codes.accepted


def get_matrix(request_id):
    return {'request_id': request_id,
            'status': "In Progress",
            'key': "sample key",
            'eta': "sample eta",
            'message': "sample message",
            'links': [],
            }, requests.codes.ok

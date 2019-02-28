import json
import os
import requests
import uuid

from connexion.lifecycle import ConnexionResponse

from matrix.common.constants import MatrixFormat, MatrixRequestStatus
from matrix.common.aws.lambda_handler import LambdaHandler, LambdaName
from matrix.common.request.request_cache import RequestCache, RequestIdNotFound
from matrix.common.request.request_tracker import RequestTracker

lambda_handler = LambdaHandler()


def post_matrix(body: dict):
    has_ids = 'bundle_fqids' in body
    has_url = 'bundle_fqids_url' in body

    format = body['format'] if 'format' in body else MatrixFormat.ZARR.value
    expected_formats = [mf.value for mf in MatrixFormat]

    # Validate input parameters
    if format not in expected_formats:
        return ConnexionResponse(status_code=requests.codes.bad_request,
                                 body={
                                     'message': "Invalid parameters supplied. "
                                                "Please supply a valid `format`. "
                                                "Visit https://matrix.dev.data.humancellatlas.org for more information."
                                 })
    if has_ids and has_url:
        return ConnexionResponse(status_code=requests.codes.bad_request,
                                 body={
                                     'message': "Invalid parameters supplied. "
                                                "Please supply either one of `bundle_fqids` or `bundle_fqids_url`. "
                                                "Visit https://matrix.dev.data.humancellatlas.org for more information."
                                 })

    if not has_ids and not has_url:
        return ConnexionResponse(status_code=requests.codes.bad_request,
                                 body={
                                     'message': "Missing required parameter. "
                                                "One of `bundle_fqids` or `bundle_fqids_url` must be supplied. "
                                                "Visit https://matrix.dev.data.humancellatlas.org for more information."
                                 })

    if not has_url and len(json.dumps(body['bundle_fqids'])) > 128000:
        return ConnexionResponse(status_code=requests.codes.request_entity_too_large,
                                 body={
                                     'message': "List of bundle fqids is too large. "
                                                "Consider using bundle_fqids_url instead. "
                                                "Visit https://matrix.dev.data.humancellatlas.org for more information."
                                 })

    if has_url:
        bundle_fqids_url = body['bundle_fqids_url']
        bundle_fqids = None
    else:
        bundle_fqids = body['bundle_fqids']
        bundle_fqids_url = None

    request_id = str(uuid.uuid4())
    RequestCache(request_id).initialize()
    driver_payload = {
        'request_id': request_id,
        'bundle_fqids': bundle_fqids,
        'bundle_fqids_url': bundle_fqids_url,
        'format': format,
    }
    lambda_handler.invoke(LambdaName.DRIVER, driver_payload)

    return ConnexionResponse(status_code=requests.codes.accepted,
                             body={
                                 'request_id': request_id,
                                 'status': MatrixRequestStatus.IN_PROGRESS.value,
                                 'matrix_location': "",
                                 'eta': "",
                                 'message': "Job started.",
                             })


def get_matrix(request_id: str):

    # There are a few cases to handle here. First, if the request_id is not in
    # the cache table at all, then this id has never been made and we should
    # 404.
    try:
        request_hash = RequestCache(request_id).retrieve_hash()
    except RequestIdNotFound:
        return ConnexionResponse(status_code=requests.codes.not_found,
                                 body={'message': f"Unable to find job with request ID {request_id}."})

    # But, there are two other states we need to handle:
    # 1. The request_id has been written into the cache table but the driver
    #    function hasn't started yet. Then there's no hash associated with the
    #    id.
    # 2. The driver function for the request_hash has started, but it hasn't
    #    yet written anything into the output or state tables. In this case
    #    queries against those table won't return anything.

    in_progress_response = ConnexionResponse(
        status_code=requests.codes.ok,
        body={
            'request_id': request_id,
            'status': MatrixRequestStatus.IN_PROGRESS.value,
            'matrix_location': "",
            'eta': "",
            'message': f"Request {request_id} has been accepted and is currently being "
                       f"processed. Please try again later.",
        })

    # Check for case 1
    if request_hash is None:
        # If the id is in the hash table, but the hash isn't in the request
        # table, it just means the driver hasn't run yet
        return in_progress_response

    request_tracker = RequestTracker(request_hash)

    # Check for case 2
    if not request_tracker.is_initialized:
        return in_progress_response

    format = request_tracker.format

    # Failed case
    if request_tracker.error:
        return ConnexionResponse(status_code=requests.codes.ok,
                                 body={
                                     'request_id': request_id,
                                     'status': MatrixRequestStatus.FAILED.value,
                                     'matrix_location': "",
                                     'eta': "",
                                     'message': request_tracker.error,
                                 })
    # Complete case
    elif request_tracker.is_request_complete():
        s3_results_bucket = os.environ['S3_RESULTS_BUCKET']

        matrix_location = ""
        if format == MatrixFormat.ZARR.value:
            matrix_location = f"s3://{s3_results_bucket}/{request_hash}.{format}"
        elif format == MatrixFormat.LOOM.value:
            matrix_location = f"https://s3.amazonaws.com/{s3_results_bucket}/{request_hash}.{format}"
        elif format == MatrixFormat.CSV.value or format == MatrixFormat.MTX.value:
            matrix_location = f"https://s3.amazonaws.com/{s3_results_bucket}/{request_hash}.{format}.zip"

        return ConnexionResponse(status_code=requests.codes.ok,
                                 body={
                                     'request_id': request_id,
                                     'status': MatrixRequestStatus.COMPLETE.value,
                                     'matrix_location': matrix_location,
                                     'eta': "",
                                     'message': f"Request {request_id} has successfully completed. "
                                                f"The resultant expression matrix is available for download at "
                                                f"{matrix_location}",
                                 })
    # In progress case
    return ConnexionResponse(status_code=requests.codes.ok,
                             body={
                                 'request_id': request_id,
                                 'status': MatrixRequestStatus.IN_PROGRESS.value,
                                 'matrix_location': "",
                                 'eta': "",
                                 'message': f"Request {request_id} has been accepted and is currently being processed. "
                                            f"Please try again later.",
                             })


def get_formats():
    return ConnexionResponse(status_code=requests.codes.ok,
                             body=[item.value for item in MatrixFormat])

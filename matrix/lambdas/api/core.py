import json
import os
import requests
import uuid

from connexion.lifecycle import ConnexionResponse

from matrix.common import query_constructor
from matrix.common.exceptions import MatrixException
from matrix.common.constants import MatrixFormat, MatrixRequestStatus
from matrix.common.config import MatrixInfraConfig
from matrix.common.aws.lambda_handler import LambdaHandler, LambdaName
from matrix.common.request.request_tracker import RequestTracker
from matrix.common.aws.sqs_handler import SQSHandler

lambda_handler = LambdaHandler()
sqs_handler = SQSHandler()
matrix_infra_config = MatrixInfraConfig()


def post_matrix(body: dict):

    format_ = body['format'] if 'format' in body else MatrixFormat.LOOM.value
    expected_formats = [mf.value for mf in MatrixFormat]

    # Validate input parameters
    if format_ not in expected_formats:
        return ({'message': "Invalid parameters supplied. "
                            "Please supply a valid `format`. "
                            "Visit https://matrix.dev.data.humancellatlas.org for more information."},
                requests.codes.bad_request)
    if "filter" not in body:
        return ({'message': "Invalid parameters supplied. "
                            "Please supply a filter. "
                            "Visit https://matrix.dev.data.humancellatlas.org for more information."},
                requests.codes.bad_request)

    if len(json.dumps(body["filter"])) > 128000:
        return ({'message': "The filter specification is too large. "
                            "Visit https://matrix.dev.data.humancellatlas.org for more information."},
                requests.codes.request_entity_too_large)

    request_id = str(uuid.uuid4())
    RequestTracker(request_id).initialize_request(format_)

    driver_payload = {
        'request_id': request_id,
        'filter': body["filter"],
        'fields': body.get("fields", query_constructor.DEFAULT_FIELDS),
        'feature': body.get("feature", query_constructor.DEFAULT_FEATURE)
    }
    lambda_handler.invoke(LambdaName.DRIVER, driver_payload)

    return ({'request_id': request_id,
             'status': MatrixRequestStatus.IN_PROGRESS.value,
             'matrix_url': "",
             'eta': "",
             'message': "Job started."},
            requests.codes.accepted)


def get_matrix(request_id: str):

    # There are a few cases to handle here. First, if the request_id is not in
    # the state table at all, then this id has never been made and we should
    # 404.
    request_tracker = RequestTracker(request_id)
    if not request_tracker.is_initialized:
        return ({'message': f"Unabge to find job with request ID {request_id}."},
                requests.codes.not_found)

    in_progress_response = (
        {'request_id': request_id,
         'status': MatrixRequestStatus.IN_PROGRESS.value,
         'matrix_url': "",
         'eta': "",
         'message': f"Request {request_id} has been accepted and is currently being "
                    f"processed. Please try again later."},
        requests.codes.ok)

    # if the request tracker is not able to retrieve the format,
    # it means that the driver has not created the relevant entry in the output table yet.
    try:
        format = request_tracker.format
    except MatrixException:
        return in_progress_response

    # Failed case
    if request_tracker.error:
        return ({'request_id': request_id,
                 'status': MatrixRequestStatus.FAILED.value,
                 'matrix_location': "",
                 'eta': "",
                 'message': request_tracker.error},
                requests.codes.ok)
    # Check for failed batch conversion job
    elif request_tracker.batch_job_status and request_tracker.batch_job_status == "FAILED":
        request_tracker.log_error("The matrix conversion as a part of the request has failed. \
            Please retry or contact an hca admin for help.")
        return ({'request_id': request_id,
                 'status': MatrixRequestStatus.FAILED.value,
                 'matrix_location': "",
                 'eta': "",
                 'message': request_tracker.error},
                requests.codes.ok)

    # Complete case
    elif request_tracker.is_request_complete():
        matrix_results_bucket = os.environ['MATRIX_RESULTS_BUCKET']

        matrix_location = ""
        if format == MatrixFormat.LOOM.value:
            matrix_location = f"https://s3.amazonaws.com/{matrix_results_bucket}/{request_id}.{format}"
        elif format == MatrixFormat.CSV.value or format == MatrixFormat.MTX.value:
            matrix_location = f"https://s3.amazonaws.com/{matrix_results_bucket}/{request_id}.{format}.zip"

        return ({'request_id': request_id,
                 'status': MatrixRequestStatus.COMPLETE.value,
                 'matrix_url': matrix_location,
                 'eta': "",
                 'message': f"Request {request_id} has successfully completed. "
                            f"The resultant expression matrix is available for download at "
                            f"{matrix_location}"},
                requests.codes.ok)

    # Timeout case
    elif request_tracker.timeout:

        return ({'request_id': request_id,
                 'status': MatrixRequestStatus.FAILED.value,
                 'matrix_location': "",
                 'eta': "",
                 'message': request_tracker.error},
                requests.codes.ok)
    else:
        return in_progress_response


def get_filters():
    return ConnexionResponse(status_code=requests.codes.ok,
                             body={})


def get_filter_detail(filter_name: str):
    return ConnexionResponse(status_code=requests.codes.ok,
                             body={})


def get_formats():
    return ConnexionResponse(status_code=requests.codes.ok,
                             body=[item.value for item in MatrixFormat])


def get_format_detail(format_name: str):
    return ConnexionResponse(status_code=requests.codes.ok,
                             body={})


def get_fields():
    return ConnexionResponse(status_code=requests.codes.ok,
                             body={})


def get_field_detail(filter_name: str):
    return ConnexionResponse(status_code=requests.codes.ok,
                             body={})


def get_features():
    return ConnexionResponse(status_code=requests.codes.ok,
                             body={})


def get_feature_detail(filter_name: str):
    return ConnexionResponse(status_code=requests.codes.ok,
                             body={})


def dss_notification(body):
    bundle_uuid = body['match']['bundle_uuid']
    bundle_version = body['match']['bundle_version']
    subscription_id = body['subscription_id']
    event_type = body['event_type']

    payload = {
        'bundle_uuid': bundle_uuid,
        'bundle_version': bundle_version,
        'event_type': event_type,
    }
    queue_url = matrix_infra_config.notification_q_url
    sqs_handler.add_message_to_queue(queue_url, payload)

    return ConnexionResponse(status_code=requests.codes.ok,
                             body=f"Received notification from subscription {subscription_id}: "
                                  f"{event_type} {bundle_uuid}.{bundle_version}")

import os
import requests
import uuid

from connexion.lifecycle import ConnexionResponse

from matrix.common.constants import MatrixFormat
from matrix.common.constants import MatrixRequestStatus
from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import DynamoTable
from matrix.common.dynamo_handler import StateTableField
from matrix.common.exceptions import MatrixException
from matrix.common.lambda_handler import LambdaHandler
from matrix.common.lambda_handler import LambdaName


def post_matrix(body: dict):
    has_ids = 'bundle_fqids' in body
    has_url = 'bundle_fqids_url' in body

    format = body['format'] if 'format' in body else MatrixFormat.ZARR.value

    # Validate input parameters
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

    # TODO: test this when URL param is supported
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

    return ConnexionResponse(status_code=requests.codes.accepted,
                             body={
                                 'request_id': request_id,
                                 'status': MatrixRequestStatus.IN_PROGRESS.value,
                                 'matrix_location': "",
                                 'eta': "",
                                 'message': "Job started.",
                             })


def get_matrix(request_id: str):
    try:
        dynamo_handler = DynamoHandler()
        job_state = dynamo_handler.get_table_item(DynamoTable.STATE_TABLE, request_id)
    except MatrixException as ex:
        return ConnexionResponse(status_code=ex.status,
                                 body={'message': f"Unable to find job with request ID {request_id}."})

    completed_reducer_executions = job_state[StateTableField.COMPLETED_REDUCER_EXECUTIONS.value]
    expected_converter_executions = job_state[StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value]
    completed_converter_executions = job_state[StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value]
    if completed_reducer_executions == 1 and expected_converter_executions == completed_converter_executions:
        # TODO: handle missing/corrupted zarr
        s3_key = f"s3://{os.environ['S3_RESULTS_BUCKET']}/{request_id}.zarr"
        return ConnexionResponse(status_code=requests.codes.ok,
                                 body={
                                     'request_id': request_id,
                                     'status': MatrixRequestStatus.COMPLETE.value,
                                     'matrix_location': s3_key,
                                     'eta': "",
                                     'message': f"Request {request_id} has successfully completed. "
                                                f"The resultant expression matrix is available for download at the S3 "
                                                f"location {s3_key}",
                                 })

    return ConnexionResponse(status_code=requests.codes.ok,
                             body={
                                 'request_id': request_id,
                                 'status': MatrixRequestStatus.IN_PROGRESS.value,
                                 'matrix_location': "",
                                 'eta': "",
                                 'message': f"Request {request_id} has been accepted and is currently being processed. "
                                            f"Please try again later.",
                             })

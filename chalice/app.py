import json
import traceback
import requests

from aws_xray_sdk.core import xray_recorder, patch
from aws_xray_sdk.core.context import Context
from botocore.exceptions import ClientError
from chalice import Chalice, Response
from chalicelib import rand_uuid
from chalicelib.config import logger
from chalicelib.matrix_handler import LoomMatrixHandler
from chalicelib.request_handler import RequestHandler, RequestStatus
from chalicelib.sqs import SqsQueueHandler
from chalicelib.error import ApiException, matrix_service_handler


app = Chalice(app_name='matrix-service-api')
app.debug = True

# AWS X-Ray configuration
patch(('boto3',))

xray_recorder.configure(
    service='matrix-service',
    dynamic_naming='*.execute-api.us-east-1.amazonaws.com/dev*',
    context=Context()
)

# TODO (matt w): enable flask middleware when project is converted to connexion
# from aws_xray_sdk.ext.flask.middleware import XRayMiddleware
# XRayMiddleware(app, xray_recorder)

# Replace handler here for supporting concatenation on other matrix formats
mtx_handler = LoomMatrixHandler()


@app.route('/matrices/health', methods=['GET'])
@matrix_service_handler
def health():
    return {'status': 'OK'}


@app.route('/matrices/concat/{request_id}', methods=['GET'])
@matrix_service_handler
def check_request_status(request_id):
    """
    Check the status of a matrices concatenation request based on
    a request ID.

    :param request_id: Matrices concatenation request ID.
    """
    maybe_merge_request_body = RequestHandler.get_request_attributes(request_id=request_id)

    if not maybe_merge_request_body:
        raise ApiException(status=requests.codes.not_found, code="not_found", title='request id does not exist')

    return maybe_merge_request_body


@app.route('/matrices/concat', methods=['POST'])
@matrix_service_handler
def concat_matrices():
    """
    Concat matrices within bundles based on bundles uuid
    """
    request = app.current_request
    bundle_uuids = request.json_body
    request_id = RequestHandler.generate_request_id(bundle_uuids)
    response = {"request_id": request_id}

    logger.info(f'Request ID({request_id}): Received request for concatenating matrices within bundles {bundle_uuids}.')

    job_id = rand_uuid()

    try:
        # put only if the job does not exist or is aborted
        RequestHandler.put_request(
            bundle_uuids=bundle_uuids,
            request_id=request_id,
            job_id=job_id,
            status=RequestStatus.INITIALIZED,
            ConditionExpression=f'attribute_not_exists(request_id) OR request_status = :status',
            ExpressionAttributeValues={
                ':status': RequestStatus.ABORT.name
            }
        )
        SqsQueueHandler.send_msg_to_ms_queue(
            payload=dict(
                bundle_uuids=bundle_uuids,
                job_id=job_id
            )
        )
        logger.info(f'Request ID({request_id}): Send request_id({request_id}) to SQS Queue.')
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == 'ConditionalCheckFailedException':
            return response

    return Response(body=response, status_code=201)


def ms_sqs_queue_listener(event, context):
    """
    Create a lambda function that listens for the matrix service's SQS
    queue events.Once it detects an incoming message on the queue, it
    will process it by launching a job to do the matrices concatenation
    based on the message content.
    :param event: SQS Queue event.
    """
    logger.info(f'Handling {len(event["Records"])} matrix service SQS queue events......')

    for record in event["Records"]:
        record_body = json.loads(record["body"])
        bundle_uuids = record_body["bundle_uuids"]
        request_id = RequestHandler.generate_request_id(bundle_uuids)

        """
        Check whether job id has a corresponding match in request item in the dynamodb table:
        If yes, then proceed the matrices concatenation.
        If no, it means either of the following cases happens:
          1. Message sent to SQS Queue has been corrupted; As a result,
             the job id sent by API Gateway + Lambda is different from
             the one received by the SQS queue + Lambda.
          2. Before receiving the msg from SQS queue, a duplicate mtx
             concatenation request has been initialized. As a result,
             the job id in the status file in s3 has been updated to
             a new value.
        Therefore, we need to stall the matrices concatenation to avoid
        either doing duplicate concatenation or concatenation on incorrect
        set of matrices.
        """
        try:
            job_id = RequestHandler.get_request_job_id(request_id=request_id)

            # Stall concatenation process if any of these cases described above happens
            if not job_id or job_id != record_body["job_id"]:
                return

            mtx_handler.run_merge_request(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=record_body["job_id"]
            )
        except Exception as e:
            logger.exception(traceback.format_exc())

    logger.info(f'Done.')


def ms_dead_letter_queue_listener(event, context):
    """
    A lambda function which listens on the matrix service dead letter queue.
    It will set the corresponding request status for all messages in the
    queue to ABORT.
    :param event: SQS event.
    """
    for record in event["Records"]:
        record_body = json.loads(record["body"])
        bundle_uuids = record_body["bundle_uuids"]
        job_id = record_body["job_id"]
        request_id = RequestHandler.generate_request_id(bundle_uuids)

        try:
            RequestHandler.put_request(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=job_id,
                status=RequestStatus.ABORT,
                reason_to_abort=f'Lambda function timed out.'
            )
        except:
            logger.exception(traceback.format_exc())

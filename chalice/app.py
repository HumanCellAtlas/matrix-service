import json
import traceback

from functools import wraps
from chalice import Chalice, NotFoundError, ChaliceViewError
from chalicelib.config import logger
from chalicelib.matrix_handler import LoomMatrixHandler
from chalicelib.request_handler import RequestHandler, RequestStatus
from chalicelib.sqs import SqsQueueHandler

app = Chalice(app_name='matrix-service-api')
app.debug = True

# Replace handler here for supporting concatenation on other matrix formats
mtx_handler = LoomMatrixHandler()


def api_endpoint_decorator(api_endpoint_func):
    @wraps(api_endpoint_func)
    def api_endpoint_wrapper(*args, **kwargs):
        try:
            return api_endpoint_func(*args, **kwargs)
        except KeyError:
            raise NotFoundError(f'{kwargs} has not been initialized.')
        except:
            raise ChaliceViewError(traceback.format_exc())
    return api_endpoint_wrapper


@app.route('/matrices/health', methods=['GET'])
@api_endpoint_decorator
def health():
    return {'status': 'OK'}


@app.route('/matrices/concat/{request_id}', methods=['GET'])
@api_endpoint_decorator
def check_request_status(request_id):
    """
    Check the status of a matrices concatenation request based on
    a request ID.

    :param request_id: Matrices concatenation request ID.
    """
    merge_request_body = RequestHandler.get_request_attributes(request_id=request_id)
    return merge_request_body


@app.route('/matrices/concat', methods=['POST'])
@api_endpoint_decorator
def concat_matrices():
    """
    Concat matrices within bundles based on bundles uuid
    """
    request = app.current_request
    bundle_uuids = request.json_body
    request_id = RequestHandler.generate_request_id(bundle_uuids)

    logger.info(f'Request ID({request_id}): Received request for concatenating matrices within bundles {bundle_uuids}.')

    try:
        merge_request_status = RequestHandler.get_request_status(request_id=request_id)

        logger.info(f'Request({request_id}) status: {merge_request_status}.')

        # Send the request to sqs queue if the request has been abort before
        if merge_request_status == RequestStatus.ABORT.name:
            SqsQueueHandler.send_msg_to_ms_queue(
                bundle_uuids=bundle_uuids,
                request_id=request_id
            )

    # Request does not exist
    except KeyError:
        SqsQueueHandler.send_msg_to_ms_queue(
            bundle_uuids=bundle_uuids,
            request_id=request_id
        )

    return {"request_id": request_id}


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
        except:
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

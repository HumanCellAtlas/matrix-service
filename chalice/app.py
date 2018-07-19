import json
import traceback

from chalice import Chalice, NotFoundError, BadRequestError
from hca.util import SwaggerAPIException
from cloud_blobstore import BlobNotFoundError, BlobStoreUnknownError
from chalicelib.config import MS_SQS_QUEUE_NAME, logger
from chalicelib.matrix_handler import LoomMatrixHandler
from chalicelib.request_handler import RequestHandler, RequestStatus
from chalicelib.sqs import SqsQueueHandler

app = Chalice(app_name='matrix-service-api')
app.debug = True

# Replace handler here for supporting concatenation on other matrix formats
mtx_handler = LoomMatrixHandler()


@app.route('/matrices/health', methods=['GET'])
def health():
    return {'status': 'OK'}


@app.route('/matrices/concat/{request_id}', methods=['GET'])
def check_request_status(request_id):
    """
    Check the status of a matrices concatenation request based on
    a request ID.

    :param request_id: Matrices concatenation request ID.
    """
    try:
        merge_request = RequestHandler.get_request(request_id=request_id)
        merge_request_body = json.loads(merge_request)
        return merge_request_body

    except BlobNotFoundError:
        raise NotFoundError("Request({}) has not been initialized.".format(request_id))

    except BlobStoreUnknownError:
        raise BadRequestError(traceback.format_exc())


@app.route('/matrices/concat', methods=['POST'])
def concat_matrices():
    """
    Concat matrices within bundles based on bundles uuid
    """
    request = app.current_request
    bundle_uuids = request.json_body
    request_id = RequestHandler.generate_request_id(bundle_uuids)

    logger.info("Request ID({}): Received request for concatenating matrices within bundles {};"
                .format(request_id, str(bundle_uuids)))

    try:
        merge_request = RequestHandler.get_request(request_id=request_id)
        merge_request_body = json.loads(merge_request)
        merge_request_status = merge_request_body["status"]

        logger.info("Request({}) status: {}.".format(request_id, merge_request_status))

        # Send the request to sqs queue if the request has been abort before
        if merge_request_status == RequestStatus.ABORT:
            SqsQueueHandler.send_msg_to_ms_queue(bundle_uuids=bundle_uuids, request_id=request_id)

    except BlobNotFoundError:
        # Send the request to sqs queue if the request has not been made before
        SqsQueueHandler.send_msg_to_ms_queue(bundle_uuids=bundle_uuids, request_id=request_id)

    except BlobStoreUnknownError:
        error_msg = traceback.format_exc()
        raise BadRequestError(error_msg)

    return {"request_id": request_id}


def ms_sqs_queue_listener(event, context):
    """
    Create a lambda function that listens for the matrix service's SQS
    queue events.Once it detects an incoming message on the queue, it
    will process it by launching a job to do the matrices concatenation
    based on the message content.
    :param event: SQS Queue event.
    """
    for record in event["Records"]:
        record_body = json.loads(record["body"])
        bundle_uuids = record_body["bundle_uuids"]
        request_id = RequestHandler.generate_request_id(bundle_uuids)

        """
        Check whether job id has a corresponding match in s3:
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
            merge_request = RequestHandler.get_request(request_id=request_id)
            merge_request_body = json.loads(merge_request)
            job_id = merge_request_body["job_id"]

            # Stall concatenation process if any of these cases described above happens
            if not job_id or job_id != record_body["job_id"]:
                return

            mtx_handler.run_merge_request(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=record_body["job_id"]
            )

        except (BlobStoreUnknownError, SwaggerAPIException, Exception):
            error_msg = traceback.format_exc()
            logger.exception(error_msg)

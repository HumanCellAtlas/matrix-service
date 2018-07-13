import json
import traceback

from chalice import Chalice, NotFoundError, BadRequestError, ChaliceViewError
from hca.util import SwaggerAPIException

from cloud_blobstore import BlobNotFoundError, BlobStoreUnknownError
from chalicelib import rand_uuid, logger
from chalicelib.config import MS_SQS_QUEUE_NAME, SQS_QUEUE_MSG
from chalicelib.matrix_handler import LoomMatrixHandler
from chalicelib.request_handler import RequestHandler, RequestStatus
from chalicelib.sqs import SqsQueueHandler

app = Chalice(app_name='matrix-service')
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

    :param request_id: <string> Matrices concatenation request ID
    """
    try:
        request_status = RequestHandler.check_request_status(request_id)
        logger.info("Request({}) status: {}.".format(request_id, request_status.name))

        if request_status == RequestStatus.UNINITIALIZED:
            raise NotFoundError("Request({}) has not been initialized.".format(request_id))
        else:
            mtx_url = mtx_handler.get_mtx_url(request_id)

            return {
                "status": request_status.name,
                "url": mtx_url
            }
    except (BlobNotFoundError, BlobStoreUnknownError):
        error_msg = traceback.format_exc()
        raise BadRequestError(error_msg)


@app.route('/matrices/concat', methods=['POST'])
def concat_matrices():
    """
    Concat matrices within bundles based on bundles uuid
    """
    request = app.current_request
    bundle_uuids = request.json_body
    request_id = RequestHandler.generate_request_id(bundle_uuids)

    logger.info("Request ID({}): Received request for concatenating matrices from bundles {};"
                .format(request_id, str(bundle_uuids)))

    try:
        request_status = RequestHandler.check_request_status(request_id)

        logger.info("Request({}) status: {}.".format(request_id, request_status.name))

        # Send the request as a message to the SQS queue if the request
        # has not been made or has been aborted before
        if request_status == RequestStatus.UNINITIALIZED \
                or request_status == RequestStatus.ABORT:
            job_id = rand_uuid()

            logger.info("Request ID({}): Initialize the request with job id({})"
                        .format(request_id, job_id))

            RequestHandler.update_request_status(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=job_id,
                status=RequestStatus.INITIALIZED
            )

            # Create message to send to the SQS Queue
            msg = SQS_QUEUE_MSG.copy()
            msg["bundle_uuids"] = bundle_uuids
            msg["job_id"] = job_id

            logger.info("Request ID({}): Send request message({}) to SQS Queue."
                        .format(request_id, str(msg)))

            # Send the msg to the SQS queue
            msg_str = json.dumps(msg, sort_keys=True)
            SqsQueueHandler.send_msg_to_ms_queue(msg_str)

    except BlobStoreUnknownError:
        error_msg = traceback.format_exc()
        raise BadRequestError(error_msg)
    except AssertionError:
        raise ChaliceViewError("Message has not been correctly sent to SQS Queue.")

    return {"request_id": request_id}


@app.on_sqs_message(queue=MS_SQS_QUEUE_NAME)
def ms_sqs_queue_listener(event):
    """
    Create a lambda function that listens for the matrix service's SQS
    queue events.Once it detects an incoming message on the queue, it
    will process it by launching a job to do the matrices concatenation
    based on the message content.
    :param event: SQS Queue event.
    """
    for record in event:
        msg = json.loads(record.body)
        bundle_uuids = msg["bundle_uuids"]
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
            job_id = RequestHandler.get_request_job_id(request_id=request_id)

            # Stall concatenation process if any of these cases happens
            if not job_id or job_id != msg["job_id"]:
                return

            mtx_handler.run_merge_request(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=msg["job_id"]
            )

        except (BlobStoreUnknownError, SwaggerAPIException):
            error_msg = traceback.format_exc()
            logger.exception(error_msg)

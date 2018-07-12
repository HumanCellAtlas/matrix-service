import json
import traceback

from chalice import app
from cloud_blobstore import BlobNotFoundError, BlobStoreUnknownError
from chalicelib import rand_uuid
from chalicelib.constants import MS_SQS_QUEUE_NAME, SQS_QUEUE_MSG
from chalicelib.matrix_handler import LoomMatrixHandler
from chalicelib.request_handler import RequestHandler, RequestStatus
from chalicelib.sqs import SqsQueueHandler

app = app.Chalice(app_name='matrix-service')
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
        app.log.info("Checking request({}) status.".format(request_id))
        request_status = RequestHandler.check_request_status(request_id)
        app.log.info("Request({}) status: {}.".format(request_id, request_status))

        if request_status == RequestStatus.UNINITIALIZED:
            raise app.NotFoundError("Request({}) does not exist.".format(request_id))
        else:
            app.log.info("Fetching matrix url for request({}).".format(request_id))
            mtx_url = mtx_handler.get_mtx_url(request_id)
            app.log.info("Matrix url for request({}) is: {}.".format(request_id, mtx_url))

            return {
                "status": request_status.name,
                "url": mtx_url
            }
    except (BlobNotFoundError, BlobStoreUnknownError):
        error_msg = traceback.format_exc()
        raise app.BadRequestError(error_msg)


@app.route('/matrices/concat', methods=['POST'])
def concat_matrices():
    """
    Concat matrices within bundles based on bundles uuid
    """
    request = app.current_request
    bundle_uuids = request.json_body
    request_id = RequestHandler.generate_request_id(bundle_uuids)

    try:
        request_status = RequestHandler.check_request_status(request_id)

        # Send the request as a message to the SQS queue if the request
        # has not been made before
        if request_status == RequestStatus.UNINITIALIZED:
            job_id = rand_uuid()

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

            # Send the msg to the SQS queue
            msg_str = json.dumps(msg, sort_keys=True)
            SqsQueueHandler.send_msg_to_ms_queue(msg_str)

    except BlobStoreUnknownError:
        error_msg = traceback.format_exc()
        raise app.BadRequestError(error_msg)
    except AssertionError:
        raise app.ChaliceViewError("Message has not been correctly sent to SQS Queue.")

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

        except BlobStoreUnknownError:
            error_msg = traceback.format_exc()
            app.log.exception(error_msg)
            return

        mtx_handler.run_merge_request(
            bundle_uuids=bundle_uuids,
            request_id=request_id,
            job_id=msg["job_id"]
        )

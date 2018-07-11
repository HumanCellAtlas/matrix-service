import json
import traceback

from chalice import app
from botocore.exceptions import ClientError
from chalicelib.constants import MS_SQS_QUEUE_NAME
from chalicelib.matrix_handler import LoomMatrixHandler
from chalicelib.request_handler import RequestHandler, RequestStatus
from chalicelib.sqs_queue_handler import SqsQueueHandler

app = app.Chalice(app_name='matrix-service')

# Replace handler here for supporting concatenation on other matrix formats
mtx_handler = LoomMatrixHandler()


@app.route('/matrices/health')
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
    except ClientError:
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

        # Launch the matrix creation job in the background if the request
        # has not been made before
        if request_status == RequestStatus.UNINITIALIZED:
            RequestHandler.update_request_status(
                bundle_uuids,
                request_id,
                RequestStatus.RUNNING
            )

            # Send the request as a msg to a SQS queue
            msg = json.dumps(bundle_uuids, sort_keys=True)
            SqsQueueHandler.send_msg(msg)

    except ClientError:
        error_msg = traceback.format_exc()
        raise app.BadRequestError(error_msg)

    return {"request_id": request_id}


@app.on_sqs_message(queue=MS_SQS_QUEUE_NAME)
def ms_sqs_queue_listener(event):
    """
    Create a lambda function that listens for the matrix service's SQS
    queue events.
    :param event:
    :return:
    """
    pass

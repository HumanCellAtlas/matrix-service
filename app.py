import multiprocessing
import traceback

from botocore.exceptions import ClientError
from chalice import Chalice, BadRequestError, NotFoundError
from chalicelib import logger
from chalicelib.matrix_handler import LoomMatrixHandler
from chalicelib.request_handler import RequestHandler, RequestStatus

app = Chalice(app_name='matrix-service')
# app.debug = True

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
        logger.debug("Checking request({}) status.".format(request_id))
        request_status = RequestHandler.check_request_status(request_id)
        logger.debug("Request({}) status: {}.".format(request_id, request_status))

        if request_status == RequestStatus.UNINITIALIZED:
            raise NotFoundError("Request({}) does not exist.".format(request_id))
        else:
            logger.debug("Fetching matrix url for request({}).".format(request_id))
            mtx_url = mtx_handler.get_mtx_url(request_id)
            logger.debug("Matrix url for request({}) is: {}.".format(request_id, mtx_url))

            return {
                "status": request_status.name,
                "url": mtx_url
            }
    except ClientError:
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

    try:
        request_status = RequestHandler.check_request_status(request_id)

        # Launch the matrix creation job in the background if the request
        # has not been made before
        if request_status == RequestStatus.UNINITIALIZED:
            RequestHandler.update_request_status(
                bundle_uuids,
                request_id,
                RequestStatus.RUNNING.name
            )

            # TODO: Send the request to another async service(SQS queue)
            proc = multiprocessing.Process(
                target=mtx_handler.run_merge_request,
                args=(bundle_uuids, request_id)
            )
            proc.daemon = True
            proc.start()
    except ClientError:
        error_msg = traceback.format_exc()
        raise BadRequestError(error_msg)

    return {"request_id": request_id}

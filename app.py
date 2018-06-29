import traceback

from botocore.exceptions import ClientError
from chalice import Chalice, BadRequestError, NotFoundError
from chalicelib import logger
from chalicelib.matrix_handler import LoomMatrixHandler
from chalicelib.request_handler import RequestHandler, RequestStatus

app = Chalice(app_name='matrix-service')
app.debug = True

mtx_handler = LoomMatrixHandler()


@app.route('/')
def index():
    return {'hello': 'world'}


@app.route('/matrices/concat/{request_id}')
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

    # TODO: Query DSS GET /v1/bundles for bundle manifest of each (async)

    # TODO: Filter for the matrix files and concat them to a new file (async)

    # TODO: Return a request ID
    pass

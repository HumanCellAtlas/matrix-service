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

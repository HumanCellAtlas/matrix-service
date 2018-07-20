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

@app.route('/matrices/health', methods=['GET'])
def health():
    return {'status': 'OK'}

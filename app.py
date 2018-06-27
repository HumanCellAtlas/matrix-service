from chalice import Chalice

from util.matrix_handler import LoomMatrixHandler

app = Chalice(app_name='matrix-service')

matrixHandler = LoomMatrixHandler()


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
    pass


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

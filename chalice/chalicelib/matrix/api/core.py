import requests


def post_matrix():
    return "post_matrix", requests.codes.ok


def get_matrix(request_id):
    return {'request_id': request_id,
            'status': "In Progress",
            'key': "key",
            'eta': "eta",
            'message': "message",
            'links': [],
            }, requests.codes.ok

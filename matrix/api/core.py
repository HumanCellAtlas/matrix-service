import requests


def post_matrix():
    return "post_matrix", requests.codes.ok


def get_matrix(request_id):
    return {'request_id': request_id,
            'status': "In Progress",
            'key': "sample key",
            'eta': "sample eta",
            'message': "sample message",
            'links': [],
            }, requests.codes.ok

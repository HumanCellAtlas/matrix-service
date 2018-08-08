import functools
import traceback

import requests
from chalice import Response


class ApiException(Exception):
    def __init__(self, status: int, code: str, title: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.status = status
        self.code = code
        self.message = title


def matrix_service_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ApiException as ex:
            status = ex.status
            code = ex.code
            title = ex.message
            stacktrace = traceback.format_exc()
        except Exception as ex:
            status = requests.codes.server_error
            code = "unhandled_exception"
            title = str(ex)
            stacktrace = traceback.format_exc()

        return Response(
            status_code=status,
            headers={
                'Content-Type': 'application/problem+json'
            },
            body={
                'status': status,
                'code': code,
                'title': title,
                'stacktrace': stacktrace
            }
        )
    return wrapper

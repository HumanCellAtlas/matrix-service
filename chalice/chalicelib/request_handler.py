import hashlib
import boto3
import typing

from enum import Enum
from typing import List
from chalicelib.config import REQUEST_STATUS_TABLE


class RequestStatus(Enum):
    INITIALIZED = "INITIALIZED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    ABORT = "ABORT"


class RequestHandler:
    _dynamodb = boto3.resource('dynamodb')
    _request_status_table = _dynamodb.Table(REQUEST_STATUS_TABLE)

    @staticmethod
    def generate_request_id(bundle_uuids: List[str]) -> str:
        """
        Generate a request id based on a list of bundle uuids.
        :param bundle_uuids: A list of bundle uuids.
        :return: Request id.
        """
        bundle_uuids.sort()
        m = hashlib.sha256()

        for bundle_uuid in bundle_uuids:
            m.update(bundle_uuid.encode())

        return m.hexdigest()

    @staticmethod
    def get_request_attribute(request_id: str, attribute_name: str) -> typing.Optional[str]:
        """
        Get a attribute value from the request status table based on the request_id.
        :param request_id: Matrices concatenation request id.
        :param attribute_name: Target attribute name.
        :return: Corresponding attribute value.
        """
        response = RequestHandler._request_status_table.get_item(
            Key={
                'request_id': request_id,
            },
            ProjectionExpression=f'{attribute_name}',
            ConsistentRead=True
        )
        return response['Item'].get(attribute_name)

    @staticmethod
    def get_request_status(request_id: str) -> str:
        """
        Get request status based on the request id.
        :param request_id: Matrices concatenation request id.
        :return: Request status.
        """
        return RequestHandler.get_request_attribute(request_id=request_id, attribute_name='request_status')

    @staticmethod
    def get_request_job_id(request_id: str) -> str:
        """
        Get request job id based on the request id.
        :param request_id: Matrices concatenation request id.
        :return: Request job id.
        """
        return RequestHandler.get_request_attribute(request_id=request_id, attribute_name='job_id')

    @staticmethod
    def get_request_attributes(request_id: str) -> typing.Optional[dict]:
        """
        Get all attributes values from request status table based on the request_id.
        :param request_id: Matrices concatenation request id.
        :return: All attributes values.
        """
        response = RequestHandler._request_status_table.get_item(
            Key={
                'request_id': request_id
            },
            ConsistentRead=True
        )
        return response.get('Item')

    @staticmethod
    def put_request(bundle_uuids: List[str], request_id: str, job_id: str, status: RequestStatus,
                    reason_to_abort="undefined", merged_mtx_url="undefined", **kwargs) -> None:
        """
        Update a request status items in the request status table if exists. Otherwise, create a new item.
        :param bundle_uuids: A list of bundle uuids.
        :param request_id: Matrices concatenation request id.
        :param job_id: Matrices concatenation job id.
        :param status: Request status to update.
        :param reason_to_abort: Request abort reason.
        :param merged_mtx_url: S3 url for accessing the merged matrix.
        """
        item = {
            'request_id': request_id,
            'job_id': job_id,
            'bundle_uuids': bundle_uuids,
            'request_status': status.name,
            'reason_to_abort': reason_to_abort,
            'merged_mtx_url': merged_mtx_url
        }

        put_kwargs = dict(Item=item)
        put_kwargs.update(kwargs)

        RequestHandler._request_status_table.put_item(**put_kwargs)

    @staticmethod
    def delete_request(request_id: str) -> None:
        """
        Delete the item from the request status table based on the request_id.
        :param request_id: Matrices concatenation request id.
        """
        RequestHandler._request_status_table.delete_item(
            Key={
                'request_id': request_id
            }
        )

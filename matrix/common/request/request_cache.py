import hashlib
import typing
from datetime import timedelta

from matrix.common import date
from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, CacheTableField
from matrix.common.aws.cloudwatch_handler import CloudwatchHandler, MetricName
from matrix.common.logging import Logging
from matrix.common.request.request_tracker import RequestTracker

logger = Logging.get_logger(__name__)

# Special value representing no hash. Dynamo doesn't like empty fields.
NULL_REQUEST_HASH = "null"


class RequestIdNotFound(Exception):
    """Exception for request ids missing in the request cache table."""
    pass


class RequestCache(object):
    """Interface to request caching and hashing.

    Parameters
    ----------
    request_id : uuid
        id of the request to calculate and look up hashed for

    Methods
    -------
    initialize()
        Create an empty entry for the request id in the cache table

    retrieve_hash()
        Look up the hash associated with the request id

    set_hash(bundle_fqids, format)
        Calculate, set, and return the hash value for the request id based on
        the request's bundles and format.
    """

    def __init__(self, request_id: str) -> None:
        self._request_id = request_id
        self._dynamo_handler = DynamoHandler()
        self._cloudwatch_handler = CloudwatchHandler()

        self._creation_date = ""

    @staticmethod
    def _hash_request(bundle_fqids: typing.List[str], format_: str) -> str:
        """Calculate the hash of a request defined by fqids and a requested format."""
        hash_obj = hashlib.sha256()

        for fqid in sorted(bundle_fqids):
            hash_obj.update(fqid.encode())
        if format_:
            hash_obj.update(format_.encode())

        return hash_obj.hexdigest()

    @property
    def creation_date(self):
        """"""
        if not self._creation_date:
            item = self._dynamo_handler.get_table_item(DynamoTable.CACHE_TABLE, request_id=self._request_id)
            self._creation_date = item[CacheTableField.CREATION_DATE.value]
        return self._creation_date

    @property
    def timeout(self) -> bool:
        timeout = date.to_datetime(self.creation_date) < date.get_datetime_now() - timedelta(hours=1)
        if timeout:
            RequestTracker(self.retrieve_hash()).log_error("This request has timed out after 1 hour."
                                                           "Please try again by resubmitting the POST request.")
        return timeout

    def initialize(self) -> None:
        """Initialize the request id in the request cache table.

        Sets a null hash value.
        """
        self._dynamo_handler.write_request_hash(self._request_id, NULL_REQUEST_HASH)
        self._cloudwatch_handler.put_metric_data(
            metric_name=MetricName.REQUEST,
            metric_value=1
        )

    def retrieve_hash(self) -> typing.Union[str, None]:
        """Look up the hash value for the request.

        Returns None if the request has been initialized but the hash has not yet
        been set.

        Raises RequestIdNotFound if the request id isn't in the cache table.
        """
        request_hash = self._dynamo_handler.get_request_hash(self._request_id)

        # If the request_id isn't present at all, raise
        if not request_hash:
            raise RequestIdNotFound(f"Request {self._request_id} was not found.")

        # If the request_id is there but has a null hash, it means the driver
        # hasn't run yet.
        if request_hash == NULL_REQUEST_HASH:
            return None

        # Otherwise return the real hash
        return request_hash

    def set_hash(self, bundle_fqids, format_):
        """Calculate, set, and return the hash for this request."""
        request_hash = self._hash_request(bundle_fqids, format_)
        self._dynamo_handler.write_request_hash(self._request_id, request_hash)
        return request_hash

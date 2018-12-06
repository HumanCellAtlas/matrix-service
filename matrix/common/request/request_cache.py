import hashlib
import secrets
import string
import typing

from matrix.common.aws.dynamo_handler import DynamoHandler
from matrix.common.logging import Logging

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

    @staticmethod
    def _hash_request(bundle_fqids: typing.List[str], format_: str, deterministic=True) -> str:
        """Calculate the hash of a request defined by fqids and a requested format."""
        hash_obj = hashlib.sha256()

        if not deterministic:
            salt = RequestCache._generate_salt(32)
            hash_obj.update(salt.encode())

        for fqid in sorted(bundle_fqids):
            hash_obj.update(fqid.encode())
        if format_:
            hash_obj.update(format_.encode())

        return hash_obj.hexdigest()

    @staticmethod
    def _generate_salt(size: int) -> str:
        return ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(size))

    def initialize(self) -> None:
        """Initialize the request id in the request cache table.

        Sets a null hash value.
        """
        self._dynamo_handler.write_request_hash(self._request_id, NULL_REQUEST_HASH)

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

    def set_hash(self, bundle_fqids, format_, deterministic=True):
        """Calculate, set, and return the hash for this request."""
        request_hash = self._hash_request(bundle_fqids, format_, deterministic)
        self._dynamo_handler.write_request_hash(self._request_id, request_hash)
        return request_hash

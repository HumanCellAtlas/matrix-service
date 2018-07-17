import base64
import binascii
import datetime
import typing

from google.cloud.exceptions import NotFound
from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket
from requests.exceptions import ConnectTimeout, ReadTimeout

from . import BlobNotFoundError, BlobStore, PagedIter, BlobStoreTimeoutError


def CatchTimeouts(meth):
    def wrapped(*args, **kwargs):
        try:
            return meth(*args, **kwargs)
        except (ConnectTimeout, ReadTimeout) as ex:
            raise BlobStoreTimeoutError(ex)
    return wrapped


class GSPagedIter(PagedIter):
    def __init__(
            self,
            bucket_obj: Bucket,
            *,
            prefix: str=None,
            delimiter: str=None,
            start_after_key: str=None,
            token: str=None,
            k_page_max: int=None
    ) -> None:
        self.bucket_obj = bucket_obj
        self.start_after_key = start_after_key
        self.token = token

        self.kwargs = dict()  # type: dict

        if prefix is not None:
            self.kwargs['prefix'] = prefix

        if delimiter is not None:
            self.kwargs['delimiter'] = delimiter

        if k_page_max is not None:
            self.kwargs['max_results'] = k_page_max

    @CatchTimeouts
    def get_api_response(self, next_token=None):
        kwargs = self.kwargs.copy()

        if next_token is not None:
            kwargs['page_token'] = next_token

        resp = self.bucket_obj.list_blobs(**kwargs)

        return resp

    def get_listing_from_response(self, resp):
        return (b.name for b in resp)

    def get_next_token_from_response(self, resp):
        return resp.next_page_token


class GSBlobStore(BlobStore):
    def __init__(self, gcp_client) -> None:
        super(GSBlobStore, self).__init__()

        self.gcp_client = gcp_client
        self.bucket_map = dict()  # type: typing.MutableMapping[str, Bucket]

    @classmethod
    def from_auth_credentials(cls, json_keyfile_path: str) -> "GSBlobStore":
        return cls(Client.from_service_account_json(json_keyfile_path))

    def _ensure_bucket_loaded(self, bucket: str):
        cached_bucket_obj = self.bucket_map.get(bucket, None)
        if cached_bucket_obj is not None:
            return cached_bucket_obj
        bucket_obj = self.gcp_client.bucket(bucket)  # type: Bucket
        self.bucket_map[bucket] = bucket_obj
        return bucket_obj

    @CatchTimeouts
    def list(
            self,
            bucket: str,
            prefix: str=None,
            delimiter: str=None,
    ) -> typing.Iterator[str]:
        """
        Returns an iterator of all blob entries in a bucket that match a given prefix.  Do not return any keys that
        contain the delimiter past the
        prefix.
        """
        kwargs = dict()
        if prefix is not None:
            kwargs['prefix'] = prefix
        if delimiter is not None:
            kwargs['delimiter'] = delimiter
        bucket_obj = self._ensure_bucket_loaded(bucket)
        for blob_obj in bucket_obj.list_blobs(**kwargs):
            yield blob_obj.name

    def list_v2(
            self,
            bucket: str,
            prefix: str=None,
            delimiter: str=None,
            start_after_key: str=None,
            token: str=None,
            k_page_max: int=None
    ) -> typing.Iterable[str]:
        return GSPagedIter(
            self._ensure_bucket_loaded(bucket),
            prefix=prefix,
            delimiter=delimiter,
            start_after_key=start_after_key,
            token=token,
            k_page_max=k_page_max
        )

    @CatchTimeouts
    def generate_presigned_GET_url(
            self,
            bucket: str,
            key: str,
            **kwargs) -> str:
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.get_blob(key)
        return blob_obj.generate_signed_url(datetime.timedelta(days=1))

    @CatchTimeouts
    def upload_file_handle(
            self,
            bucket: str,
            key: str,
            src_file_handle: typing.BinaryIO,
            content_type: str=None,
            metadata: dict=None):
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.blob(key, chunk_size=1 * 1024 * 1024)
        blob_obj.upload_from_file(src_file_handle, content_type=content_type)
        if metadata:
            blob_obj.metadata = metadata
            blob_obj.patch()

    @CatchTimeouts
    def delete(self, bucket: str, key: str):
        """
        Deletes an object in a bucket.  If the operation definitely did not delete anything, return False.  Any other
        return value is treated as something was possibly deleted.
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.get_blob(key)
        if blob_obj is None:
            return False
        blob_obj.delete()

    @CatchTimeouts
    def get(self, bucket: str, key: str) -> bytes:
        """
        Retrieves the data for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param key: the key of the object for which metadata is being
        retrieved.
        :return: the data
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.blob(key)

        try:
            return blob_obj.download_as_string()
        except NotFound:
            raise BlobNotFoundError(f"Could not find gs://{bucket}/{key}")

    @CatchTimeouts
    def get_cloud_checksum(
            self,
            bucket: str,
            key: str
    ) -> str:
        """
        Retrieves the cloud-provided checksum for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param key: the key of the object for which checksum is being retrieved.
        :return: the cloud-provided checksum
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.get_blob(key)
        if blob_obj is None:
            raise BlobNotFoundError(f"Could not find gs://{bucket}/{key}")

        return binascii.hexlify(base64.b64decode(blob_obj.crc32c)).decode("utf-8").lower()

    @CatchTimeouts
    def get_content_type(
            self,
            bucket: str,
            key: str
    ) -> str:
        """
        Retrieves the content-type for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param key: the key of the object for which content-type is being retrieved.
        :return: the content-type
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.get_blob(key)
        if blob_obj is None:
            raise BlobNotFoundError(f"Could not find gs://{bucket}/{key}")

        return blob_obj.content_type

    @CatchTimeouts
    def get_copy_token(
            self,
            bucket: str,
            key: str,
            cloud_checksum: str,
    ) -> typing.Any:
        """
        Given a bucket, key, and the expected cloud-provided checksum, retrieve a token that can be passed into
        :func:`~cloud_blobstore.BlobStore.copy` that guarantees the copy refers to the same version of the blob
        identified by the checksum.
        :param bucket: the bucket the object resides in.
        :param key: the key of the object for which checksum is being retrieved.
        :param cloud_checksum: the expected cloud-provided checksum.
        :return: an opaque copy token
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.get_blob(key)
        if blob_obj is None:
            raise BlobNotFoundError(f"Could not find gs://{bucket}/{key}")
        assert binascii.hexlify(base64.b64decode(blob_obj.crc32c)).decode("utf-8").lower() == cloud_checksum
        return blob_obj.generation

    @CatchTimeouts
    def get_user_metadata(
            self,
            bucket: str,
            key: str
    ) -> typing.Dict[str, str]:
        """
        Retrieves the user metadata for a given object in a given bucket.  If the platform has any mandatory prefixes or
        suffixes for the metadata keys, they should be stripped before being returned.
        :param bucket: the bucket the object resides in.
        :param key: the key of the object for which metadata is being
        retrieved.
        :return: a dictionary mapping metadata keys to metadata values.
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        response = bucket_obj.get_blob(key)
        if response is None:
            raise BlobNotFoundError(f"Could not find gs://{bucket}/{key}")
        return response.metadata

    @CatchTimeouts
    def get_size(
            self,
            bucket: str,
            key: str
    ) -> int:
        """
        Retrieves the filesize
        :param bucket: the bucket the object resides in.
        :param key: the key of the object for which size is being retrieved.
        :return: integer equal to filesize in bytes
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        response = bucket_obj.get_blob(key)
        if response is None:
            raise BlobNotFoundError(f"Could not find gs://{bucket}/{key}")
        res = response.size
        return res

    @CatchTimeouts
    def copy(
            self,
            src_bucket: str, src_key: str,
            dst_bucket: str, dst_key: str,
            copy_token: typing.Any=None,
            **kwargs
    ):
        src_bucket_obj = self._ensure_bucket_loaded(src_bucket)
        src_blob_obj = src_bucket_obj.get_blob(src_key)
        dst_bucket_obj = self._ensure_bucket_loaded(dst_bucket)
        try:
            src_bucket_obj.copy_blob(src_blob_obj, dst_bucket_obj, new_name=dst_key, source_generation=copy_token)
        except NotFound as ex:
            raise BlobNotFoundError(f"Could not find gs://{src_bucket}/{src_key}") from ex

    @CatchTimeouts
    def check_bucket_exists(self, bucket: str) -> bool:
        """
        Checks if bucket with specified name exists.
        :param bucket: the bucket to be checked.
        :return: true if specified bucket exists.
        """
        bucket_obj = self.gcp_client.bucket(bucket)  # type: Bucket
        return bucket_obj.exists()

import typing
import types


class PagedIter(typing.Iterable[str]):
    """
    Provide an iterator that will iterate over every object, filtered by prefix and delimiter. Alternately continue
    iteration with token and key (start_after_key).
    """

    def get_api_response(self, next_token):
        """
        Make blobstore-specific list api request.
        """
        raise NotImplementedError()

    def get_listing_from_response(self, resp) -> typing.Iterable[str]:
        """
        Retrieve blob key listing from blobstore response.
        """
        raise NotImplementedError()

    def get_next_token_from_response(self, resp) -> str:
        """
        Retrieve opaque continuation token from blobstore response.
        """
        raise NotImplementedError()

    def __iter__(self):
        """
        Iterate over the blobs, saving page tokens and blob key start_after_keys as needed in order to continue
        listing where one left off.

        If start_after_key is not None, iteration will begin on the next key if start_after_key is found on the
        first page of results. If it is not found on the first page of results, BlobPagingError will be raised.
        """
        next_token = self.token

        while True:
            self.token = next_token

            resp = self.get_api_response(next_token)
            listing = self.get_listing_from_response(resp)

            if self.start_after_key:
                while True:
                    try:
                        key = next(listing)
                    except StopIteration:
                        raise BlobPagingError('Marker not found in this page')

                    if key == self.start_after_key:
                        break

            while True:
                try:
                    self.start_after_key = next(listing)
                    yield self.start_after_key
                except StopIteration:
                    break

            self.start_after_key = None

            next_token = self.get_next_token_from_response(resp)

            if not next_token:
                break


class BlobStore:
    """Abstract base class for all blob stores."""
    def __init__(self):
        pass

    def list(
            self,
            bucket: str,
            prefix: str=None,
            delimiter: str=None,
    ) -> typing.Iterator[str]:
        """
        Returns an iterator of all blob entries in a bucket that match a given prefix.  Do not return any keys that
        contain the delimiter past the prefix.
        """
        raise NotImplementedError()

    def list_v2(
            self,
            bucket: str,
            prefix: str=None,
            delimiter: str=None,
            start_after_key: str=None,
            token: str=None,
            k_page_max: int=None
    ) -> typing.Iterable[str]:
        """
        Returns an iterator of all blob entries in a bucket that match a given prefix.  Do not return any keys that
        contain the delimiter past the prefix.
        """
        raise NotImplementedError()

    def generate_presigned_GET_url(
            self,
            bucket: str,
            key: str,
            **kwargs) -> str:
        # TODO: things like http ranges need to be explicit parameters.
        # users of this API should not need to know the argument names presented
        # to the cloud API.
        """
        Retrieves a presigned URL for the given HTTP method for blob at `key`. Raises BlobNotFoundError if the blob
        is not present.
        """
        raise NotImplementedError()

    def upload_file_handle(
            self,
            bucket: str,
            key: str,
            src_file_handle: typing.BinaryIO,
            content_type: str=None,
            metadata: dict=None):
        """
        Saves the contents of a file handle as the contents of an object in a bucket.
        """
        raise NotImplementedError()

    def delete(self, bucket: str, key: str):
        """
        Deletes an object in a bucket.  If the operation definitely did not delete anything, return False.  Any other
        return value is treated as something was possibly deleted.
        """
        raise NotImplementedError()

    def get(self, bucket: str, key: str) -> bytes:
        """
        Retrieves the data for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param key: the key of the object for which metadata is being
        retrieved.
        :return: the data
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    def copy(
            self,
            src_bucket: str, src_key: str,
            dst_bucket: str, dst_key: str,
            copy_token: typing.Any=None,
            **kwargs):
        raise NotImplementedError()

    def check_bucket_exists(self, bucket: str) -> bool:
        """
        Checks if bucket with specified name exists.
        :param bucket: the bucket to be checked.
        :return: true if specified bucket exists.
        """
        raise NotImplementedError()

    def get_bucket_region(self, bucket) -> str:
        """
        Get region associated with a specified bucket name.
        :param bucket: the bucket to be checked.
        :return: region in which specified bucket resides.
        """
        raise NotImplementedError()


class BlobStoreError(Exception):
    pass


class BlobStoreUnknownError(BlobStoreError):
    pass


class BlobStoreCredentialError(BlobStoreError):
    pass


class BlobStoreTimeoutError(BlobStoreError):
    """
    BlobStoreTimeoutError wraps timeout errors from cloud providers.
    For instance, boto3 provides `read_timeout` and `connect_timeout` configurations that may
    lead to `ConnectTimeout` and `ReadTimeout` exceeptions.
    """
    pass


class BlobBucketNotFoundError(BlobStoreError):
    pass


class BlobNotFoundError(BlobStoreError):
    pass


class BlobAlreadyExistsError(BlobStoreError):
    pass


class BlobPagingError(BlobStoreError):
    pass

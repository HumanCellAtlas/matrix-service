import boto3
import botocore
import requests
import typing

from boto3.s3.transfer import TransferConfig

from botocore.vendored.requests.exceptions import ConnectTimeout, ReadTimeout

from . import (
    BlobNotFoundError,
    BlobStore,
    BlobStoreCredentialError,
    BlobStoreTimeoutError,
    BlobStoreUnknownError,
    PagedIter,
)


def CatchTimeouts(meth):
    def wrapped(*args, **kwargs):
        try:
            return meth(*args, **kwargs)
        except (ConnectTimeout, ReadTimeout) as ex:
            raise BlobStoreTimeoutError(ex)
    return wrapped


class S3PagedIter(PagedIter):
    def __init__(
            self,
            bucket: str,
            *,
            prefix: str=None,
            delimiter: str=None,
            start_after_key: str=None,
            token: str=None,
            k_page_max: int=None
    ) -> None:
        self.start_after_key = start_after_key
        self.token = token

        self.kwargs = dict()  # type: dict

        self.kwargs['Bucket'] = bucket

        if prefix is not None:
            self.kwargs['Prefix'] = prefix

        if delimiter is not None:
            self.kwargs['Delimiter'] = delimiter

        if k_page_max is not None:
            self.kwargs['MaxKeys'] = k_page_max

    @CatchTimeouts
    def get_api_response(self, next_token):
        kwargs = self.kwargs.copy()

        if next_token is not None:
            kwargs['ContinuationToken'] = next_token

        resp = boto3.client('s3').list_objects_v2(**kwargs)

        return resp

    def get_listing_from_response(self, resp):
        if resp.get('Contents', None):
            contents = resp['Contents']
        else:
            contents = list()

        return (b['Key'] for b in contents)

    def get_next_token_from_response(self, resp):
        if resp['IsTruncated']:
            token = resp['NextContinuationToken']
        else:
            token = None

        return token


class S3BlobStore(BlobStore):
    def __init__(self, s3_client) -> None:
        super(S3BlobStore, self).__init__()

        self.s3_client = s3_client

    @classmethod
    def from_environment(cls):
        # verify that the credentials are valid.
        try:
            boto3.client('sts').get_caller_identity()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "InvalidClientTokenId":
                raise BlobStoreCredentialError()

        s3_client = boto3.client("s3")
        return cls(s3_client)

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
        kwargs = dict()
        if prefix is not None:
            kwargs['Prefix'] = prefix
        if delimiter is not None:
            kwargs['Delimiter'] = delimiter
        for item in (
                boto3.resource("s3").Bucket(bucket).
                objects.
                filter(**kwargs)):
            yield item.key

    def list_v2(
            self,
            bucket: str,
            prefix: str=None,
            delimiter: str=None,
            start_after_key: str=None,
            token: str=None,
            k_page_max: int=None
    ) -> typing.Iterable[str]:
        return S3PagedIter(
            bucket,
            prefix=prefix,
            delimiter=delimiter,
            start_after_key=start_after_key,
            token=token,
            k_page_max=k_page_max
        )

    def generate_presigned_GET_url(
            self,
            bucket: str,
            key: str,
            **kwargs) -> str:
        return self._generate_presigned_url(
            bucket,
            key,
            "get_object"
        )

    def _generate_presigned_url(
            self,
            bucket: str,
            key: str,
            method: str,
            **kwargs) -> str:
        args = kwargs.copy()
        args['Bucket'] = bucket
        args['Key'] = key
        return self.s3_client.generate_presigned_url(
            ClientMethod=method,
            Params=args,
        )

    @CatchTimeouts
    def upload_file_handle(
            self,
            bucket: str,
            key: str,
            src_file_handle: typing.BinaryIO,
            content_type: str=None,
            metadata: dict=None):
        extra_args = {}  # type: typing.MutableMapping[str, typing.Any]
        if content_type is not None:
            extra_args['ContentType'] = content_type
        if metadata is not None:
            extra_args['Metadata'] = metadata
        self.s3_client.upload_fileobj(
            src_file_handle,
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args
        )

    def delete(self, bucket: str, key: str):
        self.s3_client.delete_object(
            Bucket=bucket,
            Key=key
        )

    @CatchTimeouts
    def get(self, bucket: str, key: str) -> bytes:
        """
        Retrieves the data for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param key: the key of the object for which metadata is being
        retrieved.
        :return: the data
        """
        try:
            response = self.s3_client.get_object(
                Bucket=bucket,
                Key=key
            )
            return response['Body'].read()
        except botocore.exceptions.ClientError as ex:
            if ex.response['Error']['Code'] == "NoSuchKey":
                raise BlobNotFoundError(f"Could not find s3://{bucket}/{key}") from ex
            raise BlobStoreUnknownError(ex)

    @CatchTimeouts
    def get_all_metadata(
            self,
            bucket: str,
            key: str
    ) -> dict:
        """
        Retrieves all the metadata for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param key: the key of the object for which metadata is being retrieved.
        :return: the metadata
        """
        try:
            return self.s3_client.head_object(
                Bucket=bucket,
                Key=key
            )
        except botocore.exceptions.ClientError as ex:
            if str(ex.response['Error']['Code']) == \
                    str(requests.codes.not_found):
                raise BlobNotFoundError(f"Could not find s3://{bucket}/{key}") from ex
            raise BlobStoreUnknownError(ex)

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
        response = self.get_all_metadata(bucket, key)
        # hilariously, the ETag is quoted.  Unclear why.
        return response['ContentType']

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
        return cloud_checksum

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
        response = self.get_all_metadata(bucket, key)
        # hilariously, the ETag is quoted.  Unclear why.
        return response['ETag'].strip("\"")

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
        try:
            response = self.get_all_metadata(bucket, key)
            metadata = response['Metadata'].copy()

            response = self.s3_client.get_object_tagging(
                Bucket=bucket,
                Key=key,
            )
            for tag in response['TagSet']:
                key, value = tag['Key'], tag['Value']
                metadata[key] = value

            return metadata
        except botocore.exceptions.ClientError as ex:
            if str(ex.response['Error']['Code']) == \
                    str(requests.codes.not_found):
                raise BlobNotFoundError(f"Could not find s3://{bucket}/{key}") from ex
            raise BlobStoreUnknownError(ex)

    @CatchTimeouts
    def copy(
            self,
            src_bucket: str, src_key: str,
            dst_bucket: str, dst_key: str,
            copy_token: typing.Any=None,
            **kwargs
    ):
        if copy_token is not None:
            kwargs['CopySourceIfMatch'] = copy_token
        try:
            self.s3_client.copy(
                dict(
                    Bucket=src_bucket,
                    Key=src_key,
                ),
                Bucket=dst_bucket,
                Key=dst_key,
                ExtraArgs=kwargs,
                Config=TransferConfig(
                    multipart_threshold=64 * 1024 * 1024,
                    multipart_chunksize=64 * 1024 * 1024,
                ),
            )
        except botocore.exceptions.ClientError as ex:
            if str(ex.response['Error']['Code']) == str(requests.codes.precondition_failed):
                raise BlobNotFoundError(f"Could not find s3://{src_bucket}/{src_key}") from ex
            raise BlobStoreUnknownError(ex)

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
        try:
            response = self.get_all_metadata(bucket, key)
            size = response['ContentLength']
            return size
        except botocore.exceptions.ClientError as ex:
            if str(ex.response['Error']['Code']) == str(requests.codes.not_found):
                raise BlobNotFoundError(f"Could not find s3://{bucket}/{key}") from ex
            raise BlobStoreUnknownError(ex)

    def find_next_missing_parts(
            self,
            bucket: str,
            key: str,
            upload_id: str,
            part_count: int,
            search_start: int=1,
            return_count: int=1) -> typing.Sequence[int]:
        """
        Given a `bucket`, `key`, and `upload_id`, find the next N missing parts of a multipart upload, where
        N=`return_count`.  If `search_start` is provided, start the search at part M, where M=`search_start`.
        `part_count` is the number of parts expected for the upload.

        Note that the return value may contain fewer than N parts.
        """
        if part_count < search_start:
            raise ValueError("")
        result = list()
        while True:
            kwargs = dict(Bucket=bucket, Key=key, UploadId=upload_id)  # type: dict
            if search_start > 1:
                kwargs['PartNumberMarker'] = search_start - 1

            # retrieve all the parts after the one we *think* we need to start from.
            parts_resp = self.s3_client.list_parts(**kwargs)

            # build a set of all the parts known to be uploaded, detailed in this request.
            parts_map = set()  # type: typing.Set[int]
            for part_detail in parts_resp.get('Parts', []):
                parts_map.add(part_detail['PartNumber'])

            while True:
                if search_start not in parts_map:
                    # not found, add it to the list of parts we still need.
                    result.append(search_start)

                # have we met our requirements?
                if len(result) == return_count or search_start == part_count:
                    return result

                search_start += 1

                if parts_resp['IsTruncated'] and search_start == parts_resp['NextPartNumberMarker']:
                    # finished examining the results of this batch, move onto the next one
                    break

    @CatchTimeouts
    def check_bucket_exists(self, bucket: str) -> bool:
        """
        Checks if bucket with specified name exists.
        :param bucket: the bucket to be checked.
        :return: true if specified bucket exists.
        """
        exists = True
        try:
            self.s3_client.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
        return exists

    @CatchTimeouts
    def get_bucket_region(self, bucket) -> str:
        """
        Get region associated with a specified bucket name.
        :param bucket: the bucket to be checked.
        :return: region, Note that underlying AWS API returns None for default US-East-1,
        I'm replacing that with us-east-1.
        """
        region = self.s3_client.get_bucket_location(Bucket=bucket)["LocationConstraint"]
        return 'us-east-1' if region is None else region

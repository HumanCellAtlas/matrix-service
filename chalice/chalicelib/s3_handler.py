import json
import boto3
from botocore.exceptions import ClientError


class S3Handler:
    s3 = boto3.resource("s3")

    @staticmethod
    def object_exists(key, bucket_name):
        """
        Check the existence of a key in s3 bucket.
        :param key: Key to check for existence.
        :param bucket_name: S3 bucket name.
        :return: True if the key exists in the bucket; Otherwise, return false.
        """
        try:
            S3Handler.s3.Object(bucket_name=bucket_name, key=key).get()
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return False

    @staticmethod
    def delete_object(key, bucket_name):
        """
        Delete a key from s3 bucket.
        :param key: S3 bucket key.
        :param bucket_name: S3 bucket name.
        """
        if S3Handler.object_exists(key=key, bucket_name=bucket_name):
            S3Handler.s3.Object(bucket_name=bucket_name, key=key).delete()

    @staticmethod
    def get_object_body(key, bucket_name):
        """
        Get the body of a corresponding object in a bucket.
        :param key: S3 bucket key.
        :param bucket_name: S3 bucket name.
        :return: Body of the object if it exists in the bucket.
        """
        try:
            response = S3Handler.s3.Object(
                bucket_name=bucket_name,
                key=key
            ).get()

            body = json.loads(response['Body'].read())
            return body
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return None
            else:
                raise e

    @staticmethod
    def put_object(key, bucket_name, body):
        """
        Put an object into the s3 bucket.
        :param key: S3 key of the object.
        :param bucket_name: S3 bucket name.
        :param body: Body of the object.
        """
        S3Handler.s3.Object(
            bucket_name=bucket_name,
            key=key
        ).put(Body=body)

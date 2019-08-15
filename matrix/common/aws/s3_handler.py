import boto3


class S3Handler:
    """
    Interface for interacting with an S3 Bucket.
    """

    def __init__(self, bucket):
        self.s3 = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.s3_bucket = self.s3.Bucket(bucket)

    def store_content_in_s3(self, obj_key: str, content: str):
        s3_obj = self._s3_object(obj_key)
        s3_obj.put(Body=content)
        return s3_obj.key

    def load_content_from_obj_key(self, obj_key):
        obj = self._s3_object(obj_key)
        content = obj.get()['Body'].read()
        return content.decode().strip()

    def _s3_object(self, obj_key: str):
        return self.s3_bucket.Object(obj_key)

    def copy_obj(self, src_key, dst_key):
        src = {
            'Bucket': self.s3_bucket.name,
            'Key': src_key
        }
        self.s3_bucket.copy(src, dst_key)

    def ls(self, key):
        response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket.name, Prefix=key)
        return response.get('Contents', [])

    def exists(self, key):
        objects = self.ls(key)
        return len(objects) > 0

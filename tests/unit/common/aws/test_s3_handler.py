import os
import uuid

from matrix.common.aws.s3_handler import S3Handler
from tests.unit import MatrixTestCaseUsingMockAWS


class TestS3Handler(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestS3Handler, self).setUp()
        self.create_s3_queries_bucket()
        self.s3_handler = S3Handler(os.environ['MATRIX_QUERY_BUCKET'])
        self.request_id = str(uuid.uuid4())

    def test_store_content_in_s3(self):
        obj_key = f"{self.request_id}/expression"
        test_content = "test_content"

        s3_loc_path = self.s3_handler.store_content_in_s3(obj_key, test_content)

        obj = self.s3_handler.s3_bucket.Object(obj_key)
        content = obj.get()['Body'].read()
        self.assertEqual(content, b'test_content')
        self.assertEqual(s3_loc_path, f"{self.request_id}/expression")

    def test_load_content_from_obj_key(self):
        obj_key = f"{self.request_id}/expression"
        test_content = "test_content"
        self.s3_handler.store_content_in_s3(obj_key, test_content)

        content = self.s3_handler.load_content_from_obj_key(obj_key)

        self.assertEqual(content, 'test_content')

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

    def test_copy_obj(self):
        src_key = "test_key"
        dst_key = "test_key_copy"
        test_content = "test_content"

        self.s3_handler.store_content_in_s3(src_key, test_content)
        self.s3_handler.copy_obj(src_key, dst_key)

        content = self.s3_handler.load_content_from_obj_key(src_key)
        content_copy = self.s3_handler.load_content_from_obj_key(dst_key)

        self.assertEqual(content, content_copy)

    def test_ls(self):
        obj_key = "test_key"
        test_content = "test_content"

        results = self.s3_handler.ls(obj_key)
        self.assertFalse(results)

        self.s3_handler.store_content_in_s3(obj_key, test_content)
        results = self.s3_handler.ls(obj_key)
        self.assertEqual(len(results), 1)

    def test_exists(self):
        obj_key = "test_key"
        test_content = "test_content"

        self.assertFalse(self.s3_handler.exists(obj_key))

        self.s3_handler.store_content_in_s3(obj_key, test_content)
        self.assertTrue(self.s3_handler.exists(obj_key))

    def test_delete_objects(self):
        obj_key_1 = "test_key_1"
        obj_key_2 = "test_key_2"
        obj_key_3 = "test_key_3"
        test_content = "test_content"

        self.s3_handler.store_content_in_s3(obj_key_1, test_content)
        self.s3_handler.store_content_in_s3(obj_key_2, test_content)
        self.s3_handler.store_content_in_s3(obj_key_3, test_content)

        expected_keys = [obj_key_1, obj_key_2, obj_key_3]
        keys_in_s3 = [s3_obj['Key'] for s3_obj in self.s3_handler.ls("")]
        self.assertEqual(keys_in_s3, expected_keys)

        with self.subTest("Delete no objects"):
            deleted_objects = self.s3_handler.delete_objects(keys=[])

            expected_keys = [obj_key_1, obj_key_2, obj_key_3]
            keys_in_s3 = [s3_obj['Key'] for s3_obj in self.s3_handler.ls("")]
            self.assertEqual(keys_in_s3, expected_keys)
            self.assertTrue(len(deleted_objects) == 0)

        with self.subTest("Delete one object"):
            deleted_objects = self.s3_handler.delete_objects(keys=[obj_key_1])

            expected_keys = [obj_key_2, obj_key_3]
            keys_in_s3 = [s3_obj['Key'] for s3_obj in self.s3_handler.ls("")]
            self.assertEqual(keys_in_s3, expected_keys)
            self.assertEqual(deleted_objects[0]['Key'], obj_key_1)

        with self.subTest("Delete multiple objects"):
            deleted_objects = self.s3_handler.delete_objects(keys=[obj_key_2, obj_key_3])

            expected_keys = []
            keys_in_s3 = [s3_obj['Key'] for s3_obj in self.s3_handler.ls("")]
            self.assertEqual(keys_in_s3, expected_keys)
            self.assertEqual(deleted_objects[0]['Key'], obj_key_2)
            self.assertEqual(deleted_objects[1]['Key'], obj_key_3)

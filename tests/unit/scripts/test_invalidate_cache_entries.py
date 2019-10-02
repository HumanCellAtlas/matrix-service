import os
import mock

from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, RequestTableField
from matrix.common.aws.s3_handler import S3Handler
from scripts.invalidate_cache_entries import invalidate_cache_entries
from tests.unit import MatrixTestCaseUsingMockAWS


class TestInvalidateCacheEntries(MatrixTestCaseUsingMockAWS):
    def setUp(self):
        super(TestInvalidateCacheEntries, self).setUp()

        self.create_test_deployment_table()
        self.create_test_request_table()

        self.init_test_deployment_table()
        self.create_s3_results_bucket()

        self.dynamo_handler = DynamoHandler()
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_invalidate_cache_entries(self, mock_put_metric_data):
        request_hash_1 = "test_hash_1"
        request_hash_2 = "test_hash_2"
        request_id_1 = "test_id_1"
        request_id_2 = "test_id_2"
        request_id_3 = "test_id_3"
        request_id_4 = "test_id_4"
        test_format = "test_format"
        test_content = "test_content"

        s3_key_1 = f"0/{request_hash_1}/{request_id_1}.{test_format}"
        s3_key_2 = f"0/{request_hash_1}/{request_id_2}.{test_format}"
        s3_key_3 = f"0/{request_hash_2}/{request_id_3}.{test_format}"
        s3_key_4 = f"0/{request_hash_2}/{request_id_4}.{test_format}"

        dynamo_handler = DynamoHandler()
        dynamo_handler.create_request_table_entry(request_id_1, test_format)
        dynamo_handler.create_request_table_entry(request_id_2, test_format)
        dynamo_handler.create_request_table_entry(request_id_3, test_format)
        dynamo_handler.create_request_table_entry(request_id_4, test_format)

        dynamo_handler.set_table_field_with_value(table=DynamoTable.REQUEST_TABLE,
                                                  key=request_id_1,
                                                  field_enum=RequestTableField.REQUEST_HASH,
                                                  field_value=request_hash_1)
        dynamo_handler.set_table_field_with_value(table=DynamoTable.REQUEST_TABLE,
                                                  key=request_id_2,
                                                  field_enum=RequestTableField.REQUEST_HASH,
                                                  field_value=request_hash_1)
        dynamo_handler.set_table_field_with_value(table=DynamoTable.REQUEST_TABLE,
                                                  key=request_id_3,
                                                  field_enum=RequestTableField.REQUEST_HASH,
                                                  field_value=request_hash_2)
        dynamo_handler.set_table_field_with_value(table=DynamoTable.REQUEST_TABLE,
                                                  key=request_id_4,
                                                  field_enum=RequestTableField.REQUEST_HASH,
                                                  field_value=request_hash_2)

        s3_results_bucket_handler = S3Handler(os.environ['MATRIX_RESULTS_BUCKET'])
        s3_results_bucket_handler.store_content_in_s3(s3_key_1, test_content)
        s3_results_bucket_handler.store_content_in_s3(s3_key_2, test_content)
        s3_results_bucket_handler.store_content_in_s3(s3_key_3, test_content)
        s3_results_bucket_handler.store_content_in_s3(s3_key_4, test_content)

        self.assertTrue(s3_results_bucket_handler.exists(s3_key_1))
        self.assertTrue(s3_results_bucket_handler.exists(s3_key_2))
        self.assertTrue(s3_results_bucket_handler.exists(s3_key_3))
        self.assertTrue(s3_results_bucket_handler.exists(s3_key_4))

        invalidate_cache_entries(request_ids=[request_id_3],
                                 request_hashes=[request_hash_1])

        error_1 = dynamo_handler.get_table_item(table=DynamoTable.REQUEST_TABLE,
                                                key=request_id_1)[RequestTableField.ERROR_MESSAGE.value]
        error_2 = dynamo_handler.get_table_item(table=DynamoTable.REQUEST_TABLE,
                                                key=request_id_2)[RequestTableField.ERROR_MESSAGE.value]
        error_3 = dynamo_handler.get_table_item(table=DynamoTable.REQUEST_TABLE,
                                                key=request_id_3)[RequestTableField.ERROR_MESSAGE.value]
        error_4 = dynamo_handler.get_table_item(table=DynamoTable.REQUEST_TABLE,
                                                key=request_id_4)[RequestTableField.ERROR_MESSAGE.value]

        self.assertFalse(s3_results_bucket_handler.exists(s3_key_1))
        self.assertFalse(s3_results_bucket_handler.exists(s3_key_2))
        self.assertFalse(s3_results_bucket_handler.exists(s3_key_3))
        self.assertTrue(s3_results_bucket_handler.exists(s3_key_4))

        self.assertNotEqual(error_1, 0)
        self.assertNotEqual(error_2, 0)
        self.assertNotEqual(error_3, 0)
        self.assertEqual(error_4, 0)

import os
import uuid

import boto3

from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.zarr_to_s3_writer import ZarrToS3Writer
from .. import MatrixTestCaseUsingMockAWS


class TestZarrToS3Writer(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestZarrToS3Writer, self).setUp()

        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self.state_table_name = os.environ['DYNAMO_STATE_TABLE_NAME']
        self.output_table_name = os.environ['DYNAMO_OUTPUT_TABLE_NAME']
        self.create_test_state_table(self.dynamo)
        self.create_test_output_table(self.dynamo)

        self.request_id = str(uuid.uuid4())
        self.dynamo_handler = DynamoHandler()
        self.dynamo_handler.create_output_table_entry(self.request_id)

        self.s3_writer = ZarrToS3Writer()

    def test_get_output_row_chunk_idxs(self):
        start_chunk_idx, end_chunk_idx = self.s3_writer._get_output_row_chunk_idxs(self.request_id, 6000)
        self.assertEqual(start_chunk_idx, 0)
        self.assertEqual(end_chunk_idx, 2)

        start_chunk_idx, end_chunk_idx = self.s3_writer._get_output_row_chunk_idxs(self.request_id, 8000)
        self.assertEqual(start_chunk_idx, 2)
        self.assertEqual(end_chunk_idx, 5)

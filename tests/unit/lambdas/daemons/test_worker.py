import os
import uuid

import boto3

from ... import MatrixTestCaseUsingMockAWS
from ... import test_bundle_spec
from matrix.lambdas.daemons.worker import Worker
from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import StateTableField
from matrix.common.dynamo_handler import DynamoTable


class TestWorker(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestWorker, self).setUp()

        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])
        self.state_table_name = os.environ['DYNAMO_STATE_TABLE_NAME']
        self.output_table_name = os.environ['DYNAMO_OUTPUT_TABLE_NAME']
        self.request_id = str(uuid.uuid4())
        self.create_test_state_table(self.dynamo)
        self.create_test_output_table(self.dynamo)
        self.handler = DynamoHandler()
        self.format_string = "zarr"
        self.worker_chunk_spec = {
            "bundle_uuid": test_bundle_spec['uuid'],
            "bundle_version": test_bundle_spec['version'],
            "start_row": 100,
            "num_rows": 6500
        }
        self.worker = Worker(self.request_id)

    def test_run(self):
        pass

    def test_parse_worker_chunk_spec(self):
        self.worker._parse_worker_chunk_spec(self.worker_chunk_spec)
        self.assertEqual(self.worker._bundle_uuid, test_bundle_spec['uuid'])
        self.assertEqual(self.worker._bundle_version, test_bundle_spec['version'])
        self.assertEqual(self.worker._input_start_row, 100)
        self.assertEqual(self.worker._input_end_row, 6600)

    def test_check_if_all_workers_and_mappers_for_request_are_complete_returns_false(self):
        self.handler.create_state_table_entry(self.request_id, num_bundles=2)
        complete = self.worker._check_if_all_workers_and_mappers_for_request_are_complete(self.request_id)
        self.assertEqual(complete, False)

        field_value = StateTableField.EXPECTED_WORKER_EXECUTIONS.value
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_value, 5)
        field_value = StateTableField.COMPLETED_MAPPER_EXECUTIONS.value
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_value, 2)

        complete = self.worker._check_if_all_workers_and_mappers_for_request_are_complete(self.request_id)
        self.assertEqual(complete, False)

    def test_check_if_all_workers_and_mappers_for_request_are_complete_returns_true(self):
        self.handler.create_state_table_entry(self.request_id, num_bundles=2)
        complete = self.worker._check_if_all_workers_and_mappers_for_request_are_complete(self.request_id)
        self.assertEqual(complete, False)

        field_value = StateTableField.EXPECTED_WORKER_EXECUTIONS.value
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_value, 5)
        field_value = StateTableField.COMPLETED_MAPPER_EXECUTIONS.value
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_value, 2)
        field_value = StateTableField.COMPLETED_WORKER_EXECUTIONS.value
        self.handler.increment_table_field(DynamoTable.STATE_TABLE, self.request_id, field_value, 5)

        complete = self.worker._check_if_all_workers_and_mappers_for_request_are_complete(self.request_id)
        self.assertEqual(complete, True)

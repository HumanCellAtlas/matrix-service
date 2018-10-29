import uuid
from unittest import mock

from matrix.common.dynamo_handler import DynamoHandler, DynamoTable, StateTableField
from matrix.common.request_tracker import RequestTracker, Subtask
from tests.unit import MatrixTestCaseUsingMockAWS


class TestRequestTracker(MatrixTestCaseUsingMockAWS):
    def setUp(self):
        super(TestRequestTracker, self).setUp()

        self.request_id = str(uuid.uuid4())
        self.request_tracker = RequestTracker(self.request_id)
        self.dynamo_handler = DynamoHandler()

        self.create_test_output_table()
        self.create_test_state_table()

        self.dynamo_handler.create_state_table_entry(self.request_id, 1, "test_format")
        self.dynamo_handler.create_output_table_entry(self.request_id, "test_format")

    def test_format(self):
        self.assertEqual(self.request_tracker.format, "test_format")

    def test_error(self):
        self.assertEqual(self.request_tracker.error, "")

        self.dynamo_handler.write_request_error(self.request_id, "test error")
        self.assertEqual(self.request_tracker.error, "test error")

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.create_output_table_entry")
    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.create_state_table_entry")
    def test_init_request(self, mock_create_state_table_entry, mock_create_output_table_entry):
        self.request_tracker.init_request(1, "test_format")

        mock_create_state_table_entry.assert_called_once_with(self.request_id, 1, "test_format")
        mock_create_output_table_entry.assert_called_once_with(self.request_id, "test_format")

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.increment_table_field")
    def test_expect_subtask_execution(self, mock_increment_table_field):
        self.request_tracker.expect_subtask_execution(Subtask.DRIVER)

        mock_increment_table_field.assert_called_once_with(DynamoTable.STATE_TABLE,
                                                           self.request_id,
                                                           StateTableField.EXPECTED_DRIVER_EXECUTIONS,
                                                           1)

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.increment_table_field")
    def test_complete_subtask_execution(self, mock_increment_table_field):
        self.request_tracker.complete_subtask_execution(Subtask.DRIVER)

        mock_increment_table_field.assert_called_once_with(DynamoTable.STATE_TABLE,
                                                           self.request_id,
                                                           StateTableField.COMPLETED_DRIVER_EXECUTIONS,
                                                           1)

    def test_is_reducer_ready(self):
        self.assertFalse(self.request_tracker.is_reducer_ready())

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_MAPPER_EXECUTIONS,
                                                  1)
        self.assertTrue(self.request_tracker.is_reducer_ready())

    def test_is_request_complete(self):
        self.assertFalse(self.request_tracker.is_request_complete())

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS,
                                                  1)
        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_CONVERTER_EXECUTIONS,
                                                  1)
        self.assertTrue(self.request_tracker.is_request_complete())

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.write_request_error")
    def test_log_error(self, mock_write_request_error):
        self.request_tracker.log_error("test error")
        mock_write_request_error.assert_called_once_with(self.request_id, "test error")

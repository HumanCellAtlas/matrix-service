import unittest
import uuid
from unittest import mock

from matrix.lambdas.daemons.reducer import Reducer
from matrix.common.dynamo_handler import DynamoTable, StateTableField


class TestReducer(unittest.TestCase):
    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self.reducer = Reducer(self.request_id, "")

    @mock.patch("matrix.common.s3_zarr_store.S3ZarrStore.write_group_metadata")
    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.increment_table_field")
    def test_run(self, mock_dynamo_increment_table_field, mock_write_group_metadata):
        self.reducer.run()

        mock_write_group_metadata.assert_called_once_with()
        mock_dynamo_increment_table_field.assert_called_once_with(DynamoTable.STATE_TABLE,
                                                                  self.request_id,
                                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS,
                                                                  1)

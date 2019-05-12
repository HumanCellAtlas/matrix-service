import unittest
import uuid
from unittest import mock

from matrix.common.request.request_tracker import Subtask
from matrix.common.config import MatrixInfraConfig
from matrix.lambdas.daemons.v1.driver import Driver


class TestDriver(unittest.TestCase):
    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self._driver = Driver(self.request_id)

    @mock.patch("matrix.lambdas.daemons.v1.driver.Driver._format_and_store_queries_in_s3")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    def test_run_with_all_params(self,
                                 mock_complete_subtask_execution,
                                 mock_store_queries_in_s3):
        filter_ = {"op": "in", "field": "foo", "value": [1, 2, 3]}
        fields = ["test.field1", "test.field2"]
        feature = "gene"

        mock_store_queries_in_s3.return_value = []

        self._driver.run(filter_, fields, feature)

        mock_complete_subtask_execution.assert_called_once_with(Subtask.DRIVER)

    @mock.patch("matrix.common.aws.sqs_handler.SQSHandler.add_message_to_queue")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.set_table_field_with_value")
    def test___add_request_queries_to_sqs(self,
                                          mock_set_table_field_with_value,
                                          mock_complete_subtask_execution,
                                          mock_add_to_queue):
        config = MatrixInfraConfig()
        config.set({'query_job_q_url': "query_job_q_url"})
        self._driver.config = config
        test_query_loc = "test_path"

        self._driver._add_request_query_to_sqs(test_query_loc)

        payload = {
            'request_id': self.request_id,
            's3_obj_key': test_query_loc
        }
        mock_add_to_queue.assert_called_once_with("query_job_q_url", payload)
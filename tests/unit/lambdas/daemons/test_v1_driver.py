import unittest
import uuid
from unittest import mock

from matrix.common.constants import GenusSpecies
from matrix.common.request.request_tracker import Subtask
from matrix.common.config import MatrixInfraConfig
from matrix.lambdas.daemons.v1.driver import Driver
from matrix.docker.query_runner import QueryType


class TestDriver(unittest.TestCase):
    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self._driver = Driver(self.request_id)

    @mock.patch("matrix.lambdas.daemons.v1.driver.Driver.redshift_role_arn")
    @mock.patch("matrix.lambdas.daemons.v1.driver.Driver._add_request_query_to_sqs")
    @mock.patch("matrix.common.aws.s3_handler.S3Handler.store_content_in_s3")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    def test_run_with_all_params(self,
                                 mock_complete_subtask_execution,
                                 mock_store_content_in_s3,
                                 mock_add_to_sqs,
                                 mock_redshift_role):
        filter_ = {"op": "in", "field": "foo", "value": [1, 2, 3]}
        fields = ["test.field1", "test.field2"]
        feature = "gene"

        mock_store_content_in_s3.return_value = "s3_key"
        mock_redshift_role.return_value = "redshift_role"

        self._driver.run(filter_, fields, feature, GenusSpecies.HUMAN.value)

        mock_complete_subtask_execution.assert_called_once_with(Subtask.DRIVER)
        self.assertEqual(mock_store_content_in_s3.call_count, 3)

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

        self._driver._add_request_query_to_sqs(QueryType.CELL, test_query_loc)

        payload = {
            'request_id': self.request_id,
            's3_obj_key': test_query_loc,
            'type': "cell"
        }
        mock_add_to_queue.assert_called_once_with("query_job_q_url", payload)

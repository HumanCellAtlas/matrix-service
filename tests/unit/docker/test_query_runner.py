from unittest import mock
import uuid

from matrix.docker.query_runner import QueryRunner
from matrix.common.aws.sqs_handler import SQSHandler
from tests.unit import MatrixTestCaseUsingMockAWS
from matrix.common.request.request_tracker import Subtask


class TestQueryRunner(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestQueryRunner, self).setUp()
        self.query_runner = QueryRunner()
        self.matrix_infra_config.set(self.__class__.TEST_CONFIG)
        self.query_runner.matrix_infra_config = self.matrix_infra_config
        self.sqs_handler = SQSHandler()
        self.sqs.meta.client.purge_queue(QueueUrl="test_query_job_q_name")

    @mock.patch("matrix.common.aws.s3_handler.S3Handler.load_content_from_obj_key")
    @mock.patch("matrix.common.aws.sqs_handler.SQSHandler.receive_messages_from_queue")
    def test_run__with_no_messages_in_queue(self, mock_receive_messages, mock_load_obj):
        mock_receive_messages.return_value = None
        self.query_runner.run(max_loops=1)
        mock_receive_messages.assert_called_once_with(self.query_runner.query_job_q_url)
        mock_load_obj.assert_not_called()

    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.schedule_matrix_conversion")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_ready_for_conversion")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.run_query")
    @mock.patch("matrix.common.aws.s3_handler.S3Handler.load_content_from_obj_key")
    def test_run__with_one_message_in_queue_and_not_ready_for_conversion(self,
                                                                         mock_load_obj,
                                                                         mock_run_query,
                                                                         mock_complete_subtask,
                                                                         mock_is_ready_for_conversion,
                                                                         mock_schedule_conversion):
        request_id = str(uuid.uuid4())
        payload = {
            'request_id': request_id,
            's3_obj_key': "test_s3_obj_key"
        }
        self.sqs_handler.add_message_to_queue("test_query_job_q_name", payload)
        mock_is_ready_for_conversion.return_value = False

        self.query_runner.run(max_loops=1)

        mock_load_obj.assert_called_once_with("test_s3_obj_key")
        mock_run_query.assert_called()
        mock_complete_subtask.assert_called_once_with(Subtask.QUERY)
        mock_schedule_conversion.assert_not_called()

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.format")
    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.schedule_matrix_conversion")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_ready_for_conversion")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.run_query")
    @mock.patch("matrix.common.aws.s3_handler.S3Handler.load_content_from_obj_key")
    def test_run__with_one_message_in_queue_and_ready_for_conversion(self,
                                                                     mock_load_obj,
                                                                     mock_run_query,
                                                                     mock_complete_subtask,
                                                                     mock_is_ready_for_conversion,
                                                                     mock_schedule_conversion,
                                                                     mock_request_format):
        request_id = str(uuid.uuid4())
        payload = {
            'request_id': request_id,
            's3_obj_key': "test_s3_obj_key"
        }
        self.sqs_handler.add_message_to_queue("test_query_job_q_name", payload)
        mock_is_ready_for_conversion.return_value = True

        self.query_runner.run(max_loops=1)

        mock_schedule_conversion.assert_called_once_with(request_id, mock.ANY)

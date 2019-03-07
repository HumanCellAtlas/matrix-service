import unittest
import uuid
from unittest import mock

from matrix.common.request.request_tracker import Subtask
from matrix.common.config import MatrixInfraConfig
from matrix.lambdas.daemons.driver import Driver


class TestDriver(unittest.TestCase):
    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self._bundles_per_worker = 100
        self._driver = Driver(self.request_id, self._bundles_per_worker)

    @mock.patch("matrix.lambdas.daemons.driver.Driver._format_and_store_queries_in_s3")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_output_table_entry")
    def test_run_with_ids(self,
                          mock_create_output_table,
                          mock_complete_subtask_execution,
                          mock_store_queries_in_s3):
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"
        mock_store_queries_in_s3.return_value = []

        self._driver.run(bundle_fqids, None, format)

        mock_create_output_table.assert_called_once_with(mock.ANY, len(bundle_fqids), format)
        mock_complete_subtask_execution.assert_called_once_with(Subtask.DRIVER)

    @mock.patch("matrix.lambdas.daemons.driver.Driver._format_and_store_queries_in_s3")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_output_table_entry")
    @mock.patch("requests.get")
    @mock.patch("matrix.lambdas.daemons.driver.Driver._parse_download_manifest")
    def test_run_with_url(self,
                          mock_parse_download_manifest,
                          mock_get,
                          mock_create_output_table,
                          mock_complete_subtask_execution,
                          mock_store_queries_in_s3):
        bundle_fqids_url = "test_url"
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"
        mock_store_queries_in_s3.return_value = []

        mock_parse_download_manifest.return_value = bundle_fqids
        self._driver.run(None, bundle_fqids_url, format)

        mock_parse_download_manifest.assert_called_once()
        mock_create_output_table.assert_called_once_with(mock.ANY, len(bundle_fqids), format)
        mock_complete_subtask_execution.assert_called_once_with(Subtask.DRIVER)

    def test_parse_download_manifest(self):
        test_download_manifest = "UUID\tVERSION\nbundle_id_1\tbundle_version_1\nbundle_id_2\tbundle_version_2"

        parsed = self._driver._parse_download_manifest(test_download_manifest)
        self.assertTrue(parsed, ["bundle_id_1.bundle_version_1", "bundle_id_2.bundle_version_2"])

    @mock.patch("matrix.common.aws.sqs_handler.SQSHandler.add_message_to_queue")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_output_table_entry")
    def test___add_request_queries_to_sqs(self,
                                          mock_create_output_table,
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

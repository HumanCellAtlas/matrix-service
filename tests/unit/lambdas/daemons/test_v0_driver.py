import unittest
import uuid
from unittest import mock

from matrix.common.aws.dynamo_handler import DynamoTable, RequestTableField
from matrix.common.constants import GenusSpecies
from matrix.common.request.request_tracker import Subtask
from matrix.common.config import MatrixInfraConfig
from matrix.lambdas.daemons.v0.driver import Driver
from matrix.common.query_constructor import QueryType


class TestDriver(unittest.TestCase):
    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self._bundles_per_worker = 100
        self._driver = Driver(self.request_id, self._bundles_per_worker)

    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    @mock.patch("matrix.lambdas.daemons.v0.driver.Driver._format_and_store_queries_in_s3")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.set_table_field_with_value")
    def test_run_with_ids(self,
                          mock_set_table_field_with_value,
                          mock_complete_subtask_execution,
                          mock_store_queries_in_s3,
                          mock_redshift_transaction):
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"
        mock_store_queries_in_s3.return_value = []
        mock_redshift_transaction.return_value = [[2]]

        self._driver.run(bundle_fqids, None, format, GenusSpecies.MOUSE.value)

        mock_set_table_field_with_value.assert_called_once_with(DynamoTable.REQUEST_TABLE,
                                                                mock.ANY,
                                                                RequestTableField.NUM_BUNDLES,
                                                                len(bundle_fqids))
        mock_complete_subtask_execution.assert_called_once_with(Subtask.DRIVER)

    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    @mock.patch("matrix.lambdas.daemons.v0.driver.Driver._format_and_store_queries_in_s3")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.set_table_field_with_value")
    @mock.patch("requests.get")
    @mock.patch("matrix.lambdas.daemons.v0.driver.Driver._parse_download_manifest")
    def test_run_with_url(self,
                          mock_parse_download_manifest,
                          mock_get,
                          mock_set_table_field_with_value,
                          mock_complete_subtask_execution,
                          mock_store_queries_in_s3,
                          mock_redshift_transaction):
        bundle_fqids_url = "test_url"
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"
        mock_store_queries_in_s3.return_value = []
        mock_redshift_transaction.return_value = [[2]]

        mock_parse_download_manifest.return_value = bundle_fqids
        self._driver.run(None, bundle_fqids_url, format, GenusSpecies.HUMAN.value)

        mock_parse_download_manifest.assert_called_once()
        mock_set_table_field_with_value.assert_called_once_with(DynamoTable.REQUEST_TABLE,
                                                                mock.ANY,
                                                                RequestTableField.NUM_BUNDLES,
                                                                len(bundle_fqids))
        mock_complete_subtask_execution.assert_called_once_with(Subtask.DRIVER)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.log_error")
    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    @mock.patch("matrix.lambdas.daemons.v0.driver.Driver._format_and_store_queries_in_s3")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.set_table_field_with_value")
    @mock.patch("requests.get")
    @mock.patch("matrix.lambdas.daemons.v0.driver.Driver._parse_download_manifest")
    def test_run_with_unexpected_bundles(self,
                                         mock_parse_download_manifest,
                                         mock_get,
                                         mock_set_table_field_with_value,
                                         mock_complete_subtask_execution,
                                         mock_store_queries_in_s3,
                                         mock_redshift_transaction,
                                         mock_log_error):
        bundle_fqids_url = "test_url"
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"
        mock_store_queries_in_s3.return_value = []
        mock_redshift_transaction.return_value = [[3]]

        mock_parse_download_manifest.return_value = bundle_fqids
        self._driver.run(None, bundle_fqids_url, format, GenusSpecies.MOUSE.value)

        mock_parse_download_manifest.assert_called_once()
        mock_set_table_field_with_value.assert_called_once_with(DynamoTable.REQUEST_TABLE,
                                                                mock.ANY,
                                                                RequestTableField.NUM_BUNDLES,
                                                                len(bundle_fqids))
        error_msg = "resolved bundles in request do not match bundles available in matrix service"
        mock_log_error.assert_called_once_with(error_msg)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.log_error")
    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    @mock.patch("matrix.lambdas.daemons.v0.driver.Driver._format_and_store_queries_in_s3")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.set_table_field_with_value")
    @mock.patch("requests.get")
    @mock.patch("matrix.lambdas.daemons.v0.driver.Driver._parse_download_manifest")
    def test_run_with_url_with_empty_bundles(self,
                                             mock_parse_download_manifest,
                                             mock_get,
                                             mock_set_table_field_with_value,
                                             mock_complete_subtask_execution,
                                             mock_store_queries_in_s3,
                                             mock_redshift_transaction,
                                             mock_log_error):
        bundle_fqids_url = "test_url"
        bundle_fqids = []
        format = "test_format"
        mock_store_queries_in_s3.return_value = []
        mock_redshift_transaction.return_value = [[3]]

        mock_parse_download_manifest.return_value = bundle_fqids
        self._driver.run(None, bundle_fqids_url, format, GenusSpecies.HUMAN.value)

        mock_parse_download_manifest.assert_called_once()
        error_msg = "no bundles found in the supplied bundle manifest"
        mock_log_error.assert_called_once_with(error_msg)

    def test_parse_download_manifest(self):
        test_download_manifest = "UUID\tVERSION\nbundle_id_1\tbundle_version_1\nbundle_id_2\tbundle_version_2"

        parsed = self._driver._parse_download_manifest(test_download_manifest)
        self.assertTrue(parsed, ["bundle_id_1.bundle_version_1", "bundle_id_2.bundle_version_2"])

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

    @mock.patch("matrix.lambdas.daemons.v0.driver.Driver.redshift_role_arn", new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.aws.s3_handler.S3Handler.store_content_in_s3")
    def test__format_and_store_queries_in_s3(self, mock_store_in_s3, mock_redshift_role):

        self._driver.query_results_bucket = "test_query_results_bucket"
        mock_redshift_role.return_value = "test_redshift_role_arn"

        bundle_fqids = ["id1.version", "id2.version"]

        mock_store_in_s3.return_value = "test_key"

        result = self._driver._format_and_store_queries_in_s3(bundle_fqids, GenusSpecies.HUMAN)

        self.assertDictEqual(
            {QueryType.CELL: "test_key",
             QueryType.FEATURE: "test_key",
             QueryType.EXPRESSION: "test_key"},
            result)

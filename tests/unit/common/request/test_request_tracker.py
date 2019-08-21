import hashlib
import os
import pandas
import uuid
from unittest import mock
from datetime import timedelta

from matrix.common import date
from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, RequestTableField
from matrix.common.aws.s3_handler import S3Handler
from matrix.common.constants import DEFAULT_FIELDS, DEFAULT_FEATURE
from matrix.common.request.request_tracker import RequestTracker, Subtask
from tests.unit import MatrixTestCaseUsingMockAWS
from matrix.common.aws.cloudwatch_handler import MetricName


class TestRequestTracker(MatrixTestCaseUsingMockAWS):

    @mock.patch("matrix.common.date.get_datetime_now")
    def setUp(self, mock_get_datetime_now):
        super(TestRequestTracker, self).setUp()
        self.stub_date = '2019-03-18T180907.136216Z'
        mock_get_datetime_now.return_value = self.stub_date

        self.request_id = str(uuid.uuid4())
        self.request_tracker = RequestTracker(self.request_id)
        self.dynamo_handler = DynamoHandler()

        self.create_test_request_table()
        self.create_s3_results_bucket()

        self.dynamo_handler.create_request_table_entry(self.request_id,
                                                       "test_format",
                                                       ["test_field_1", "test_field_2"],
                                                       "test_feature")

    def test_is_initialized(self):
        self.assertTrue(self.request_tracker.is_initialized)

        new_request_tracker = RequestTracker("test_uuid")
        self.assertFalse(new_request_tracker.is_initialized)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.generate_request_hash")
    def test_request_hash(self, mock_generate_request_hash):
        with self.subTest("Test skip generation in API deployments:"):
            os.environ['MATRIX_VERSION'] = "test_version"
            self.assertEqual(self.request_tracker.request_hash, "N/A")
            mock_generate_request_hash.assert_not_called()

            stored_request_hash = self.dynamo_handler.get_table_item(
                DynamoTable.REQUEST_TABLE,
                request_id=self.request_id
            )[RequestTableField.REQUEST_HASH.value]

            self.assertEqual(self.request_tracker._request_hash, "N/A")
            self.assertEqual(stored_request_hash, "N/A")

            del os.environ['MATRIX_VERSION']

        with self.subTest("Test generation and storage in Dynamo on first access"):
            mock_generate_request_hash.return_value = "test_hash"
            self.assertEqual(self.request_tracker.request_hash, "test_hash")
            mock_generate_request_hash.assert_called_once()

            stored_request_hash = self.dynamo_handler.get_table_item(
                DynamoTable.REQUEST_TABLE,
                request_id=self.request_id
            )[RequestTableField.REQUEST_HASH.value]

            self.assertEqual(self.request_tracker._request_hash, "test_hash")
            self.assertEqual(stored_request_hash, "test_hash")

        with self.subTest("Test immediate retrieval on future accesses"):
            self.assertEqual(self.request_tracker.request_hash, "test_hash")
            mock_generate_request_hash.assert_called_once()

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.request_hash",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.data_version",
                new_callable=mock.PropertyMock)
    def test_s3_results_prefix(self, mock_data_version, mock_request_hash):
        mock_data_version.return_value = "test_data_version"
        mock_request_hash.return_value = "test_request_hash"

        self.assertEqual(self.request_tracker.s3_results_prefix, "test_data_version/test_request_hash")

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.format",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.request_hash",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.data_version",
                new_callable=mock.PropertyMock)
    def test_s3_results_key(self, mock_data_version, mock_request_hash, mock_format):
        mock_data_version.return_value = "test_data_version"
        mock_request_hash.return_value = "test_request_hash"
        mock_format.return_value = "loom"

        self.assertEqual(self.request_tracker.s3_results_key,
                         f"test_data_version/test_request_hash/{self.request_id}.loom")

        mock_format.return_value = "csv"
        self.assertEqual(self.request_tracker.s3_results_key,
                         f"test_data_version/test_request_hash/{self.request_id}.csv.zip")

        mock_format.return_value = "mtx"
        self.assertEqual(self.request_tracker.s3_results_key,
                         f"test_data_version/test_request_hash/{self.request_id}.mtx.zip")

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    def test_data_version(self, mock_get_table_item):
        mock_get_table_item.return_value = {RequestTableField.DATA_VERSION.value: 0}

        with self.subTest("Test Dynamo read on first access"):
            self.assertEqual(self.request_tracker.data_version, 0)
            mock_get_table_item.assert_called_once()

        with self.subTest("Test cached access on successive reads"):
            self.assertEqual(self.request_tracker.data_version, 0)
            mock_get_table_item.assert_called_once()

    def test_format(self):
        self.assertEqual(self.request_tracker.format, "test_format")

    def test_metadata_fields(self):
        self.assertEqual(self.request_tracker.metadata_fields, ["test_field_1", "test_field_2"])

    def test_feature(self):
        self.assertEqual(self.request_tracker.feature, "test_feature")

    def test_batch_job_id(self):
        self.assertEqual(self.request_tracker.batch_job_id, None)

        field_enum = RequestTableField.BATCH_JOB_ID
        self.dynamo_handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE,
                                                       self.request_id,
                                                       field_enum,
                                                       "123-123")
        self.assertEqual(self.request_tracker.batch_job_id, "123-123")

    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.get_batch_job_status")
    def test_batch_job_status(self, mock_get_job_status):
        mock_get_job_status.return_value = "FAILED"
        field_enum = RequestTableField.BATCH_JOB_ID
        self.dynamo_handler.set_table_field_with_value(DynamoTable.REQUEST_TABLE,
                                                       self.request_id,
                                                       field_enum,
                                                       "123-123")

        self.assertEqual(self.request_tracker.batch_job_status, "FAILED")

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.num_bundles",
                new_callable=mock.PropertyMock)
    def test_num_bundles_interval(self, mock_num_bundles):
        mock_num_bundles.return_value = 0
        self.assertEqual(self.request_tracker.num_bundles_interval, "0-499")

        mock_num_bundles.return_value = 1
        self.assertEqual(self.request_tracker.num_bundles_interval, "0-499")

        mock_num_bundles.return_value = 500
        self.assertEqual(self.request_tracker.num_bundles_interval, "500-999")

        mock_num_bundles.return_value = 1234
        self.assertEqual(self.request_tracker.num_bundles_interval, "1000-1499")

    def test_creation_date(self):
        self.assertEqual(self.request_tracker.creation_date, self.stub_date)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_error(self, mock_cw_put):
        self.assertEqual(self.request_tracker.error, "")

        self.request_tracker.log_error("test error")
        self.assertEqual(self.request_tracker.error, "test error")
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST_ERROR, metric_value=1)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_request_table_entry")
    def test_initialize_request(self, mock_create_request_table_entry, mock_create_cw_metric):
        self.request_tracker.initialize_request("test_format")

        mock_create_request_table_entry.assert_called_once_with(self.request_id,
                                                                "test_format",
                                                                DEFAULT_FIELDS,
                                                                DEFAULT_FEATURE)
        mock_create_cw_metric.assert_called_once()

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.metadata_fields", new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.query.cell_query_results_reader.CellQueryResultsReader.load_results")
    @mock.patch("matrix.common.query.query_results_reader.QueryResultsReader._parse_manifest")
    def test_generate_request_hash(self, mock_parse_manifest, mock_load_results, mock_metadata_fields):
        mock_load_results.return_value = pandas.DataFrame(index=["test_cell_key_1", "test_cell_key_2"])
        mock_metadata_fields.return_value = ["test_field_1", "test_field_2"]

        h = hashlib.md5()
        h.update(self.request_tracker.feature.encode())
        h.update(self.request_tracker.format.encode())
        h.update("test_field_1".encode())
        h.update("test_field_2".encode())
        h.update("test_cell_key_1".encode())
        h.update("test_cell_key_2".encode())

        self.assertEqual(self.request_tracker.generate_request_hash(), h.hexdigest())

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.increment_table_field")
    def test_expect_subtask_execution(self, mock_increment_table_field):
        self.request_tracker.expect_subtask_execution(Subtask.DRIVER)

        mock_increment_table_field.assert_called_once_with(DynamoTable.REQUEST_TABLE,
                                                           self.request_id,
                                                           RequestTableField.EXPECTED_DRIVER_EXECUTIONS,
                                                           1)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.increment_table_field")
    def test_complete_subtask_execution(self, mock_increment_table_field):
        self.request_tracker.complete_subtask_execution(Subtask.DRIVER)

        mock_increment_table_field.assert_called_once_with(DynamoTable.REQUEST_TABLE,
                                                           self.request_id,
                                                           RequestTableField.COMPLETED_DRIVER_EXECUTIONS,
                                                           1)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.s3_results_prefix",
                new_callable=mock.PropertyMock)
    def test_lookup_cached_result(self, mock_s3_results_prefix):
        mock_s3_results_prefix.return_value = "test_prefix"
        s3_handler = S3Handler(os.environ['MATRIX_RESULTS_BUCKET'])

        with self.subTest("Do not match in S3 'directories'"):
            s3_handler.store_content_in_s3("test_prefix", "test_content")
            self.assertEqual(self.request_tracker.lookup_cached_result(), "")

        with self.subTest("Successfully retrieve a result key"):
            s3_handler.store_content_in_s3("test_prefix/test_result_1", "test_content")
            s3_handler.store_content_in_s3("test_prefix/test_result_2", "test_content")
            self.assertEqual(self.request_tracker.lookup_cached_result(), "test_prefix/test_result_1")

    def test_is_request_complete(self):
        self.assertFalse(self.request_tracker.is_request_complete())

        s3_handler = S3Handler(os.environ['MATRIX_RESULTS_BUCKET'])

        s3_handler.store_content_in_s3(
            f"{self.request_tracker.s3_results_key}/{self.request_id}.{self.request_tracker.format}", "")

        self.assertTrue(self.request_tracker.is_request_complete())

    def test_is_request_ready_for_conversion(self):
        self.assertFalse(self.request_tracker.is_request_ready_for_conversion())
        self.dynamo_handler.increment_table_field(DynamoTable.REQUEST_TABLE,
                                                  self.request_id,
                                                  RequestTableField.COMPLETED_QUERY_EXECUTIONS,
                                                  3)
        self.assertTrue(self.request_tracker.is_request_ready_for_conversion())

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_complete_request(self, mock_cw_put):
        duration = 1

        self.request_tracker.complete_request(duration)

        expected_calls = [
            mock.call(metric_name=MetricName.CONVERSION_COMPLETION, metric_value=1),
            mock.call(metric_name=MetricName.REQUEST_COMPLETION, metric_value=1),
            mock.call(metric_name=MetricName.DURATION, metric_value=duration, metric_dimensions=[
                {
                    'Name': "Number of Bundles",
                    'Value': mock.ANY
                },
                {
                    'Name': "Output Format",
                    'Value': mock.ANY
                },
            ]),
            mock.call(metric_name=MetricName.DURATION, metric_value=duration, metric_dimensions=[
                {
                    'Name': "Number of Bundles",
                    'Value': mock.ANY
                },
            ])
        ]
        mock_cw_put.assert_has_calls(expected_calls)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.log_error")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.creation_date",
                new_callable=mock.PropertyMock)
    def test_timeout(self, mock_creation_date, mock_log_error):
        # no timeout
        mock_creation_date.return_value = date.to_string(date.get_datetime_now() - timedelta(hours=35, minutes=59))
        self.assertFalse(self.request_tracker.timeout)

        # timeout
        mock_creation_date.return_value = date.to_string(date.get_datetime_now() - timedelta(hours=36, minutes=1))
        self.assertTrue(self.request_tracker.timeout)
        mock_log_error.assert_called_once()

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.set_table_field_with_value")
    def test_write_batch_job_id_to_db(self, mock_set_table_field_with_value):
        self.request_tracker.write_batch_job_id_to_db("123-123")
        mock_set_table_field_with_value.assert_called_once_with(DynamoTable.REQUEST_TABLE,
                                                                self.request_id,
                                                                RequestTableField.BATCH_JOB_ID,
                                                                "123-123")

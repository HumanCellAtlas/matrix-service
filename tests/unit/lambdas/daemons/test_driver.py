import math
import unittest
import uuid
from mock import call
from unittest import mock

from matrix.common.aws.lambda_handler import LambdaName
from matrix.common.aws.cloudwatch_handler import MetricName
from matrix.common.request.request_tracker import Subtask
from matrix.lambdas.daemons.driver import Driver


class TestDriver(unittest.TestCase):
    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self._bundles_per_worker = 100
        self._driver = Driver(self.request_id, self._bundles_per_worker)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.write_request_hash")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.initialize_request")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_run_with_ids(self,
                          mock_lambda_invoke,
                          mock_initialize_request,
                          mock_complete_subtask_execution,
                          mock_is_initialized,
                          mock_write_request_hash,
                          mock_cw_put):
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"

        mock_is_initialized.return_value = False
        self._driver.run(bundle_fqids, None, format)

        mock_is_initialized.assert_called_once()
        mock_write_request_hash.assert_called_once()
        mock_cw_put.assert_called_once_with(metric_name=MetricName.CACHE_MISS, metric_value=1)

        num_mappers = math.ceil(len(bundle_fqids) / self._bundles_per_worker)
        mock_initialize_request.assert_called_once_with(num_mappers, format)
        expected_calls = [
            call(LambdaName.MAPPER, {'request_hash': mock.ANY,
                                     'bundle_fqids': bundle_fqids})
        ]
        mock_lambda_invoke.assert_has_calls(expected_calls)
        self.assertEqual(mock_lambda_invoke.call_count, num_mappers)

        mock_complete_subtask_execution.assert_called_once_with(Subtask.DRIVER)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.write_request_hash")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.initialize_request")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    @mock.patch("requests.get")
    @mock.patch("matrix.lambdas.daemons.driver.Driver._parse_download_manifest")
    def test_run_with_url(self,
                          mock_parse_download_manifest,
                          mock_get,
                          mock_lambda_invoke,
                          mock_initialize_request,
                          mock_complete_subtask_execution,
                          mock_is_initialized,
                          mock_write_request_hash,
                          mock_cw_put):
        bundle_fqids_url = "test_url"
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"

        mock_parse_download_manifest.return_value = bundle_fqids
        mock_is_initialized.return_value = False
        self._driver.run(None, bundle_fqids_url, format)

        mock_parse_download_manifest.assert_called_once()
        mock_is_initialized.assert_called_once()
        mock_write_request_hash.assert_called_once()
        mock_cw_put.assert_called_once_with(metric_name=MetricName.CACHE_MISS, metric_value=1)

        num_mappers = math.ceil(len(bundle_fqids) / self._bundles_per_worker)
        mock_initialize_request.assert_called_once_with(num_mappers, format)
        expected_calls = [
            call(LambdaName.MAPPER, {'request_hash': mock.ANY,
                                     'bundle_fqids': bundle_fqids})
        ]
        mock_lambda_invoke.assert_has_calls(expected_calls)
        self.assertEqual(mock_lambda_invoke.call_count, num_mappers)

        mock_complete_subtask_execution.assert_called_once_with(Subtask.DRIVER)

    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.write_request_hash")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.error",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.initialize_request")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_run_cached_request(self,
                                mock_lambda_invoke,
                                mock_initialize_request,
                                mock_complete_subtask_execution,
                                mock_error,
                                mock_is_initialized,
                                mock_write_request_hash,
                                mock_cw_put):
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"

        mock_is_initialized.return_value = True
        mock_error.return_value = 0
        self._driver.run(bundle_fqids, None, format)

        mock_is_initialized.assert_called_once()
        mock_error.assert_called_once()
        mock_write_request_hash.assert_called_once()
        mock_cw_put.assert_called_once_with(metric_name=MetricName.CACHE_HIT, metric_value=1)

        self.assertEqual(mock_initialize_request.call_count, 0)
        self.assertEqual(mock_lambda_invoke.call_count, 0)
        self.assertEqual(mock_complete_subtask_execution.call_count, 0)

    def test_parse_download_manifest(self):
        test_download_manifest = "UUID\tVERSION\nbundle_id_1\tbundle_version_1\nbundle_id_2\tbundle_version_2"

        parsed = self._driver._parse_download_manifest(test_download_manifest)
        self.assertTrue(parsed, ["bundle_id_1.bundle_version_1", "bundle_id_2.bundle_version_2"])

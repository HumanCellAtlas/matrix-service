import hashlib
import os
import requests
import unittest
import uuid
from unittest import mock

from matrix.common.constants import MatrixFormat, MatrixRequestStatus
from matrix.common.aws.dynamo_handler import OutputTableField
from matrix.common.aws.lambda_handler import LambdaName
from matrix.common.aws.cloudwatch_handler import MetricName
from matrix.common.request.request_cache import RequestIdNotFound
from matrix.lambdas.api.core import post_matrix, get_matrix, get_formats


class TestCore(unittest.TestCase):

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.write_request_hash")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_post_matrix_with_ids_ok(self, mock_cw_put, mock_lambda_invoke, mock_write_request_hash):
        bundle_fqids = ["id1", "id2"]
        format = MatrixFormat.ZARR.value

        body = {
            'bundle_fqids': bundle_fqids,
            'format': format
        }

        response = post_matrix(body)
        body.update({'request_id': mock.ANY})
        body.update({'bundle_fqids_url': None})

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER, body)
        mock_write_request_hash.assert_called_once_with(mock.ANY, "null")
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST, metric_value=1)
        self.assertEqual(type(response.body['request_id']), str)
        self.assertEqual(response.body['status'], MatrixRequestStatus.IN_PROGRESS.value)
        self.assertEqual(response.status_code, requests.codes.accepted)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.write_request_hash")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_post_matrix_with_url_ok(self, mock_cw_put, mock_lambda_invoke, mock_write_request_hash):
        format = MatrixFormat.ZARR.value
        bundle_fqids_url = "test_url"

        body = {
            'bundle_fqids_url': bundle_fqids_url,
            'format': format
        }

        response = post_matrix(body)
        body.update({'request_id': mock.ANY})
        body.update({'bundle_fqids': None})

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER, body)
        mock_write_request_hash.assert_called_once_with(mock.ANY, "null")
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST, metric_value=1)
        self.assertEqual(type(response.body['request_id']), str)
        self.assertEqual(response.body['status'], MatrixRequestStatus.IN_PROGRESS.value)
        self.assertEqual(response.status_code, requests.codes.accepted)

    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_with_ids_ok_and_unexpected_format(self, mock_lambda_invoke):
        bundle_fqids = ["id1", "id2"]
        format = "fake"

        body = {
            'bundle_fqids': bundle_fqids,
            'format': format
        }
        response = post_matrix(body)
        self.assertEqual(response.status_code, requests.codes.bad_request)

    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_with_ids_and_url(self, mock_lambda_invoke):
        bundle_fqids = ["id1", "id2"]
        bundle_fqids_url = "test_url"

        body = {
            'bundle_fqids': bundle_fqids,
            'bundle_fqids_url': bundle_fqids_url
        }
        response = post_matrix(body)

        self.assertEqual(mock_lambda_invoke.call_count, 0)
        self.assertEqual(response.status_code, requests.codes.bad_request)

    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_without_ids_or_url(self, mock_lambda_invoke):
        response = post_matrix({})

        self.assertEqual(mock_lambda_invoke.call_count, 0)
        self.assertEqual(response.status_code, requests.codes.bad_request)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_request_hash")
    def test_get_matrix_not_found(self, mock_get_request_hash):
        status = 404
        message = "test_message"
        request_id = str(uuid.uuid4())
        mock_get_request_hash.side_effect = RequestIdNotFound(status, message)

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, status)
        self.assertTrue(request_id in response.body['message'])

    @mock.patch("matrix.common.request.request_cache.RequestCache.timeout",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_request_hash")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_processing(self, mock_is_request_complete, mock_get_table_item,
                                   mock_get_request_hash, mock_timeout):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = False
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "test_format"}
        mock_get_request_hash.return_value = "test_hash"
        mock_timeout.return_value = False

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['status'], MatrixRequestStatus.IN_PROGRESS.value)

    @mock.patch("matrix.common.request.request_cache.RequestCache.timeout",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_request_hash")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_timeout(self, mock_is_request_complete, mock_get_table_item,
                                mock_get_request_hash, mock_timeout):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = False
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "test_format"}
        mock_get_request_hash.return_value = "test_hash"
        mock_timeout.return_value = True

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['status'], MatrixRequestStatus.FAILED.value)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_request_hash")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_failed(self, mock_is_request_complete, mock_get_table_item,
                               mock_get_request_hash):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = False
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "test error",
                                            OutputTableField.FORMAT.value: "test_format"}
        mock_get_request_hash.return_value = "test_hash"

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['status'], MatrixRequestStatus.FAILED.value)
        self.assertEqual(response.body['message'], "test error")

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_request_hash")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_zarr_matrix_complete(self, mock_is_request_complete, mock_get_table_item,
                                      mock_get_request_hash):
        request_id = str(uuid.uuid4())
        request_hash = hashlib.sha256().hexdigest()
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "zarr"}
        mock_get_request_hash.return_value = request_hash

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['matrix_location'],
                         f"s3://{os.environ['S3_RESULTS_BUCKET']}/{request_hash}.zarr")
        self.assertEqual(response.body['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_request_hash")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_loom_matrix_complete(self, mock_is_request_complete, mock_get_table_item,
                                      mock_get_request_hash):
        request_id = str(uuid.uuid4())
        request_hash = hashlib.sha256().hexdigest()
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "loom"}
        mock_get_request_hash.return_value = request_hash

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['matrix_location'],
                         f"https://s3.amazonaws.com/{os.environ['S3_RESULTS_BUCKET']}/{request_hash}.loom")

        self.assertEqual(response.body['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_request_hash")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_csv_matrix_complete(self, mock_is_request_complete, mock_get_table_item,
                                     mock_get_request_hash):
        request_id = str(uuid.uuid4())
        request_hash = hashlib.sha256().hexdigest()
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "csv"}
        mock_get_request_hash.return_value = request_hash

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['matrix_location'],
                         f"https://s3.amazonaws.com/{os.environ['S3_RESULTS_BUCKET']}/{request_hash}.csv.zip")

        self.assertEqual(response.body['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_request_hash")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_mtx_matrix_complete(self, mock_is_request_complete, mock_get_table_item,
                                     mock_get_request_hash):
        request_id = str(uuid.uuid4())
        request_hash = hashlib.sha256().hexdigest()
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "mtx"}
        mock_get_request_hash.return_value = request_hash

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['matrix_location'],
                         f"https://s3.amazonaws.com/{os.environ['S3_RESULTS_BUCKET']}/{request_hash}.mtx.zip")

        self.assertEqual(response.body['status'], MatrixRequestStatus.COMPLETE.value)

    def test_get_formats(self):
        response = get_formats()
        self.assertEqual(response.body, [item.value for item in MatrixFormat])

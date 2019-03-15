import os
import requests
import unittest
import uuid
from unittest import mock

from matrix.common.constants import MatrixFormat, MatrixRequestStatus
from matrix.common.exceptions import MatrixException
from matrix.common.aws.dynamo_handler import OutputTableField
from matrix.common.aws.lambda_handler import LambdaName
from matrix.common.aws.cloudwatch_handler import MetricName
from matrix.lambdas.api.core import post_matrix, get_matrix, get_formats, dss_notification


class TestCore(unittest.TestCase):

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_state_table_entry")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_post_matrix_with_ids_ok(self, mock_cw_put, mock_lambda_invoke, mock_dynamo_create_state):
        bundle_fqids = ["id1", "id2"]
        format = MatrixFormat.LOOM.value

        body = {
            'bundle_fqids': bundle_fqids,
            'format': format
        }

        response = post_matrix(body)
        body.update({'request_id': mock.ANY})
        body.update({'bundle_fqids_url': None})

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER, body)
        mock_dynamo_create_state.assert_called_once_with(mock.ANY)
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST, metric_value=1)
        self.assertEqual(type(response.body['request_id']), str)
        self.assertEqual(response.body['status'], MatrixRequestStatus.IN_PROGRESS.value)
        self.assertEqual(response.status_code, requests.codes.accepted)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_state_table_entry")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_post_matrix_with_url_ok(self, mock_cw_put, mock_lambda_invoke, mock_dynamo_create_state):
        format = MatrixFormat.LOOM.value
        bundle_fqids_url = "test_url"

        body = {
            'bundle_fqids_url': bundle_fqids_url,
            'format': format
        }

        response = post_matrix(body)
        body.update({'request_id': mock.ANY})
        body.update({'bundle_fqids': None})

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER, body)
        mock_dynamo_create_state.assert_called_once_with(mock.ANY)
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

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    def test_get_matrix_not_found(self, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_get_table_item.side_effect = MatrixException(status=requests.codes.not_found, title=f"Unable to find")

        response = get_matrix(request_id)

        self.assertEqual(response.status_code, 404)
        self.assertTrue(request_id in response.body['message'])

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_processing__driver_not_initialized(self, mock_is_request_complete, mock_get_table_item,
                                                           mock_initialized):
        request_id = str(uuid.uuid4())
        mock_initialized.return_value = True
        mock_get_table_item.side_effect = MatrixException(status=requests.codes.not_found, title=f"Unable to find")

        response = get_matrix(request_id)

        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['status'], MatrixRequestStatus.IN_PROGRESS.value)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_processing__post_driver(self, mock_is_request_complete, mock_get_table_item, mock_initialized):
        request_id = str(uuid.uuid4())
        mock_initialized.return_value = True
        mock_is_request_complete.return_value = False
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "test_format"}

        response = get_matrix(request_id)

        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['status'], MatrixRequestStatus.IN_PROGRESS.value)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_failed(self, mock_is_request_complete, mock_get_table_item, mock_initialized):
        request_id = str(uuid.uuid4())
        mock_initialized.return_value = True
        mock_is_request_complete.return_value = False
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "test error",
                                            OutputTableField.FORMAT.value: "test_format"}

        response = get_matrix(request_id)

        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['status'], MatrixRequestStatus.FAILED.value)
        self.assertEqual(response.body['message'], "test error")

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_loom_matrix_complete(self, mock_is_request_complete, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "loom"}

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['matrix_location'],
                         f"https://s3.amazonaws.com/{os.environ['MATRIX_RESULTS_BUCKET']}/{request_id}.loom")

        self.assertEqual(response.body['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_csv_matrix_complete(self, mock_is_request_complete, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "csv"}

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['matrix_location'],
                         f"https://s3.amazonaws.com/{os.environ['MATRIX_RESULTS_BUCKET']}/{request_id}.csv.zip")

        self.assertEqual(response.body['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_mtx_matrix_complete(self, mock_is_request_complete, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {OutputTableField.ERROR_MESSAGE.value: "",
                                            OutputTableField.FORMAT.value: "mtx"}

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['matrix_location'],
                         f"https://s3.amazonaws.com/{os.environ['MATRIX_RESULTS_BUCKET']}/{request_id}.mtx.zip")

        self.assertEqual(response.body['status'], MatrixRequestStatus.COMPLETE.value)

    def test_get_formats(self):
        response = get_formats()
        self.assertEqual(response.body, [item.value for item in MatrixFormat])

    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_dss_notification(self, mock_lambda_invoke):
        body = {
            'subscription_id': "test_sub_id",
            'event_type': "test_event",
            'match': {
                'bundle_uuid': "test_id",
                'bundle_version': "test_version"
            }
        }
        expected_payload = {
            'bundle_uuid': "test_id",
            'bundle_version': "test_version",
            'event_type': 'test_event'
        }

        resp = dss_notification(body)
        mock_lambda_invoke.assert_called_once_with(LambdaName.NOTIFICATION, expected_payload)

        self.assertEqual(resp.status_code, requests.codes.ok)

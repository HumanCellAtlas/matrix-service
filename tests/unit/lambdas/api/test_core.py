import os
import requests
import unittest
import uuid
from unittest import mock

from matrix.common.constants import MatrixFormat
from matrix.common.constants import MatrixRequestStatus
from matrix.common.dynamo_handler import StateTableField, OutputTableField
from matrix.common.exceptions import MatrixException
from matrix.common.lambda_handler import LambdaName
from matrix.lambdas.api.core import post_matrix
from matrix.lambdas.api.core import get_matrix


class TestCore(unittest.TestCase):
    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_with_ids_ok(self, mock_lambda_invoke):
        bundle_fqids = ["id1", "id2"]
        format = MatrixFormat.ZARR.value

        body = {
            'bundle_fqids': bundle_fqids,
            'format': format
        }
        response = post_matrix(body)
        body.update({'request_id': mock.ANY})

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER, body)
        self.assertEqual(type(response.body['request_id']), str)
        self.assertEqual(response.body['status'], MatrixRequestStatus.IN_PROGRESS.value)
        self.assertEqual(response.status_code, requests.codes.accepted)

    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_with_ids_ok_and_unexpected_format(self, mock_lambda_invoke):
        bundle_fqids = ["id1", "id2"]
        format = "fake"

        body = {
            'bundle_fqids': bundle_fqids,
            'format': format
        }
        response = post_matrix(body)
        self.assertEqual(response.status_code, requests.codes.bad_request)

    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
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

    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_without_ids_or_url(self, mock_lambda_invoke):
        response = post_matrix({})

        self.assertEqual(mock_lambda_invoke.call_count, 0)
        self.assertEqual(response.status_code, requests.codes.bad_request)

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.get_table_item")
    def test_get_matrix_not_found(self, mock_get_table_item):
        status = 404
        message = "test_message"
        request_id = str(uuid.uuid4())
        mock_get_table_item.side_effect = MatrixException(status, message)

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, status)
        self.assertTrue(request_id in response.body['message'])

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.get_table_item")
    def test_get_matrix_processing(self, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_get_table_item.return_value = {
            StateTableField.COMPLETED_REDUCER_EXECUTIONS.value: 0,
            StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value: 0,
            StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value: 0,
            OutputTableField.FORMAT.value: "zarr"
        }

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['status'], MatrixRequestStatus.IN_PROGRESS.value)

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.get_table_item")
    def test_get_zarr_matrix_complete(self, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_get_table_item.return_value = {
            StateTableField.COMPLETED_REDUCER_EXECUTIONS.value: 1,
            StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value: 0,
            StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value: 0,
            OutputTableField.FORMAT.value: "zarr"
        }

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['matrix_location'], f"s3://{os.environ['S3_RESULTS_BUCKET']}/{request_id}.zarr")
        self.assertEqual(response.body['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.get_table_item")
    def test_get_loom_matrix_complete(self, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_get_table_item.return_value = {
            StateTableField.COMPLETED_REDUCER_EXECUTIONS.value: 1,
            StateTableField.COMPLETED_CONVERTER_EXECUTIONS.value: 0,
            StateTableField.EXPECTED_CONVERTER_EXECUTIONS.value: 0,
            OutputTableField.FORMAT.value: "loom"
        }

        response = get_matrix(request_id)
        self.assertEqual(response.status_code, requests.codes.ok)
        self.assertEqual(response.body['matrix_location'], f"s3://{os.environ['S3_RESULTS_BUCKET']}/{request_id}.loom")
        self.assertEqual(response.body['status'], MatrixRequestStatus.COMPLETE.value)

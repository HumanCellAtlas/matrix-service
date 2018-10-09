import requests
import unittest
from unittest import mock

from matrix.common.constants import MatrixFormat
from matrix.common.constants import MatrixRequestStatus
from matrix.common.lambda_handler import LambdaName
from matrix.lambdas.api.core import post_matrix


class TestCore(unittest.TestCase):
    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_with_ids_ok(self, mock_lambda_invoke):
        bundle_fqids = ["id1", "id2"]
        format = MatrixFormat.ZARR.value

        body = {
            'bundle_fqids': bundle_fqids,
            'format': format
        }
        response, code = post_matrix(body)
        body.update({'request_id': mock.ANY})

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER, body)
        self.assertEqual(type(response['request_id']), str)
        self.assertEqual(response['status'], MatrixRequestStatus.IN_PROGRESS.value)
        self.assertEqual(code, requests.codes.accepted)

    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_with_ids_and_url(self, mock_lambda_invoke):
        bundle_fqids = ["id1", "id2"]
        bundle_fqids_url = "test_url"

        body = {
            'bundle_fqids': bundle_fqids,
            'bundle_fqids_url': bundle_fqids_url
        }
        response, code = post_matrix(body)

        self.assertEqual(mock_lambda_invoke.call_count, 0)
        self.assertEqual(code, requests.codes.bad_request)

    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_without_ids_or_url(self, mock_lambda_invoke):
        response, code = post_matrix({})

        self.assertEqual(mock_lambda_invoke.call_count, 0)
        self.assertEqual(code, requests.codes.bad_request)

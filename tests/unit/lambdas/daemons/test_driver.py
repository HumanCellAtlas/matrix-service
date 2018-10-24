import math
import unittest
import uuid
from mock import call
from unittest import mock

from matrix.common.lambda_handler import LambdaName
from matrix.lambdas.daemons.driver import Driver


class TestDriver(unittest.TestCase):
    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self._bundles_per_worker = 100
        self._driver = Driver(self.request_id, self._bundles_per_worker)

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.create_state_table_entry")
    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.create_output_table_entry")
    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_run(self, mock_lambda_invoke, mock_dynamo_create_output_table_entry, mock_dynamo_create_state_table_entry):
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"

        self._driver.run(bundle_fqids, format)

        num_mappers = math.ceil(len(bundle_fqids) / self._bundles_per_worker)
        mock_dynamo_create_state_table_entry.assert_called_once_with(self.request_id, num_mappers, format)
        mock_dynamo_create_output_table_entry.assert_called_once_with(self.request_id, format)
        expected_calls = [
            call(LambdaName.MAPPER, {'request_id': self.request_id,
                                     'bundle_fqids': bundle_fqids})
        ]
        mock_lambda_invoke.assert_has_calls(expected_calls)
        self.assertEqual(mock_lambda_invoke.call_count, num_mappers)

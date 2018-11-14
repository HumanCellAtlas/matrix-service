import math
import unittest
import uuid
from mock import call
from unittest import mock

from matrix.common.lambda_handler import LambdaName
from matrix.common.request_tracker import Subtask
from matrix.lambdas.daemons.driver import Driver


class TestDriver(unittest.TestCase):
    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self._bundles_per_worker = 100
        self._driver = Driver(self.request_id, self._bundles_per_worker)

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.write_request_hash")
    @mock.patch("matrix.common.request_tracker.RequestTracker.is_initialized",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.request_tracker.RequestTracker.initialize_request")
    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_run(self, mock_lambda_invoke, mock_initialize_request,
                 mock_complete_subtask_execution, mock_is_initialized, mock_write_request_hash):
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"

        mock_is_initialized.return_value = False
        self._driver.run(bundle_fqids, None, format)

        mock_is_initialized.assert_called_once()
        mock_write_request_hash.assert_called_once()

        num_mappers = math.ceil(len(bundle_fqids) / self._bundles_per_worker)
        mock_initialize_request.assert_called_once_with(num_mappers, format)
        expected_calls = [
            call(LambdaName.MAPPER, {'request_hash': mock.ANY,
                                     'bundle_fqids': bundle_fqids})
        ]
        mock_lambda_invoke.assert_has_calls(expected_calls)
        self.assertEqual(mock_lambda_invoke.call_count, num_mappers)

        mock_complete_subtask_execution.assert_called_once_with(Subtask.DRIVER)

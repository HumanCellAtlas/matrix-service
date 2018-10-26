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

    @mock.patch("matrix.common.request_tracker.RequestTracker.complete_subtask_node")
    @mock.patch("matrix.common.request_tracker.RequestTracker.init_request")
    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_run(self, mock_lambda_invoke, mock_init_request, mock_complete_subtask_node):
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"

        self._driver.run(bundle_fqids, None, format)

        num_mappers = math.ceil(len(bundle_fqids) / self._bundles_per_worker)
        mock_init_request.assert_called_once_with(num_mappers, format)
        expected_calls = [
            call(LambdaName.MAPPER, {'request_id': self.request_id,
                                     'bundle_fqids': bundle_fqids})
        ]
        mock_lambda_invoke.assert_has_calls(expected_calls)
        self.assertEqual(mock_lambda_invoke.call_count, num_mappers)

        mock_complete_subtask_node.assert_called_once_with(Subtask.DRIVER)

import unittest
import uuid
from mock import call
from unittest import mock

from matrix.common.lambda_handler import LambdaName
from matrix.lambdas.daemons.driver import Driver


class TestDriver(unittest.TestCase):
    def setUp(self):
        self._driver = Driver()

    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.create_state_table_entry")
    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.create_output_table_entry")
    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_run(self, mock_lambda_invoke, mock_dynamo_create_output_table_entry, mock_dynamo_create_state_table_entry):
        request_id = str(uuid.uuid4())
        bundle_fqids = ["id1.version", "id2.version"]
        format = "test_format"

        self._driver.run(request_id, bundle_fqids, "", format)

        mock_dynamo_create_state_table_entry.assert_called_once_with(request_id, len(bundle_fqids))
        mock_dynamo_create_output_table_entry.assert_called_once_with(request_id)
        expected_calls = [
            call(LambdaName.MAPPER, {'request_id': request_id,
                                     'bundle_uuid': "id1",
                                     'bundle_version': "version",
                                     'filter_string': "",
                                     'format': format}),
            call(LambdaName.MAPPER, {'request_id': request_id,
                                     'bundle_uuid': "id2",
                                     'bundle_version': "version",
                                     'filter_string': "",
                                     'format': format}),
        ]
        mock_lambda_invoke.assert_has_calls(expected_calls)
        self.assertEqual(mock_lambda_invoke.call_count, len(bundle_fqids))

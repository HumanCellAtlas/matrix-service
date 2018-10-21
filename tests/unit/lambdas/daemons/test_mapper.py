import unittest
import uuid
from mock import call
from unittest import mock

from matrix.common.dynamo_handler import DynamoTable
from matrix.common.dynamo_handler import StateTableField
from matrix.common.dss_zarr_store import DSSZarrStore
from matrix.common.lambda_handler import LambdaName
from matrix.lambdas.daemons.mapper import Mapper


class TestMapper(unittest.TestCase):

    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self._mapper = Mapper(self.request_id)

    @mock.patch("matrix.lambdas.daemons.mapper.Mapper._get_worker_payload")
    @mock.patch("matrix.lambdas.daemons.mapper.Mapper._get_chunk_specs")
    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.increment_table_field")
    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_run_ok(self,
                    mock_lambda_invoke,
                    mock_dynamo_increment_table_field,
                    mock_get_chunk_specs,
                    mock_get_worker_payload):
        bundle_uuid = str(uuid.uuid4())
        bundle_version = "version"
        bundle_fqids = ['.'.join([bundle_uuid, bundle_version])]

        test_chunk_specs = [{'start_row': 0, 'num_rows': 5}]
        mock_get_chunk_specs.return_value = test_chunk_specs
        mock_get_worker_payload.return_value = {}

        self._mapper.run(bundle_fqids)

        mock_lambda_invoke.assert_called_with(LambdaName.WORKER, mock.ANY)
        self.assertEqual(mock_lambda_invoke.call_count, len(test_chunk_specs))

        expected_calls = [
            call(DynamoTable.STATE_TABLE,
                 self.request_id,
                 StateTableField.EXPECTED_WORKER_EXECUTIONS,
                 len(test_chunk_specs)),
            call(DynamoTable.STATE_TABLE,
                 self.request_id,
                 StateTableField.COMPLETED_MAPPER_EXECUTIONS,
                 1)
        ]
        mock_dynamo_increment_table_field.assert_has_calls(expected_calls)
        self.assertEqual(mock_dynamo_increment_table_field.call_count, 2)

    @mock.patch("matrix.lambdas.daemons.mapper.Mapper._get_chunk_specs")
    @mock.patch("matrix.common.dynamo_handler.DynamoHandler.increment_table_field")
    @mock.patch("matrix.common.lambda_handler.LambdaHandler.invoke")
    def test_run_no_chunks(self,
                           mock_lambda_invoke,
                           mock_dynamo_increment_table_field,
                           mock_get_chunk_specs):
        bundle_uuid = str(uuid.uuid4())
        bundle_version = "version"
        bundle_fqids = ['.'.join([bundle_uuid, bundle_version])]

        test_chunk_specs = []
        mock_get_chunk_specs.return_value = test_chunk_specs

        self._mapper.run(bundle_fqids)
        mock_lambda_invoke.assert_not_called()

        expected_calls = [
            call(DynamoTable.STATE_TABLE,
                 self.request_id,
                 StateTableField.COMPLETED_MAPPER_EXECUTIONS,
                 1)
        ]
        mock_dynamo_increment_table_field.assert_has_calls(expected_calls)
        self.assertEqual(mock_dynamo_increment_table_field.call_count, 1)

    def test_get_worker_payload(self):
        test_chunk_spec = []
        expected_payload = {
            'request_id': self.request_id,
            'worker_chunk_spec': test_chunk_spec
        }

        payload = self._mapper._get_worker_payload(test_chunk_spec)

        self.assertTrue("request_id" in payload)
        self.assertTrue("worker_chunk_spec" in payload)

        self.assertEqual(payload, expected_payload)

    @mock.patch("zarr.group")
    @mock.patch.object(DSSZarrStore, "__init__")
    def test_get_chunk_specs_ok(self, mock_dss_zarr_store, mock_zarr_group):
        bundle_uuid = str(uuid.uuid4())
        bundle_version = "version"
        bundle_fqids = ['.'.join([bundle_uuid, bundle_version])]
        chunk_size = 10
        nchunks = 5

        test_zarr_group = mock.Mock()
        test_expression_data = mock.Mock()
        test_expression_data.chunks = [chunk_size]
        test_expression_data.nchunks = nchunks
        test_zarr_group.expression = test_expression_data

        mock_dss_zarr_store.return_value = None
        mock_zarr_group.return_value = test_zarr_group

        chunk_specs = Mapper._get_chunk_specs(bundle_fqids)

        self.assertEqual(len(chunk_specs), nchunks)
        for i, chunk_spec in enumerate(chunk_specs):
            self.assertEqual(chunk_spec['start_row'], i * chunk_size)
            self.assertEqual(chunk_spec['num_rows'], chunk_size)

    @mock.patch("zarr.group")
    @mock.patch.object(DSSZarrStore, "__init__")
    def test_get_chunk_specs_no_chunks(self, mock_dss_zarr_store, mock_zarr_group):
        bundle_uuid = str(uuid.uuid4())
        bundle_version = "version"
        bundle_fqids = ['.'.join([bundle_uuid, bundle_version])]
        chunk_size = 10
        nchunks = 0

        test_zarr_group = mock.Mock()
        test_expression_data = mock.Mock()
        test_expression_data.chunks = [chunk_size]
        test_expression_data.nchunks = nchunks
        test_zarr_group.expression = test_expression_data

        mock_dss_zarr_store.return_value = None
        mock_zarr_group.return_value = test_zarr_group

        chunk_specs = Mapper._get_chunk_specs(bundle_fqids)

        self.assertEqual(len(chunk_specs), 0)

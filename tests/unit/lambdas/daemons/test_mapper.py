import hashlib
import unittest
import uuid

import pytest

from unittest import mock

from matrix.common.zarr.dss_zarr_store import DSSZarrStore
from matrix.common.aws.lambda_handler import LambdaName
from matrix.common.request.request_tracker import Subtask
from matrix.lambdas.daemons.mapper import Mapper


class TestMapper(unittest.TestCase):

    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self.request_hash = hashlib.sha256().hexdigest()
        self._mapper = Mapper(self.request_id, self.request_hash)

    @mock.patch("matrix.lambdas.daemons.mapper.Mapper._get_worker_payload")
    @mock.patch("matrix.lambdas.daemons.mapper.Mapper._get_chunk_specs")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.expect_subtask_execution")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_run_ok(self,
                    mock_lambda_invoke,
                    mock_complete_subtask_execution,
                    mock_expect_subtask_execution,
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

        mock_expect_subtask_execution.assert_called_once_with(Subtask.WORKER)
        mock_complete_subtask_execution.assert_called_once_with(Subtask.MAPPER)

    @mock.patch("matrix.lambdas.daemons.mapper.Mapper._get_chunk_specs")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.expect_subtask_execution")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.complete_subtask_execution")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_run_no_chunks(self,
                           mock_lambda_invoke,
                           mock_complete_subtask_execution,
                           mock_expect_subtask_execution,
                           mock_get_chunk_specs):
        bundle_uuid = str(uuid.uuid4())
        bundle_version = "version"
        bundle_fqids = ['.'.join([bundle_uuid, bundle_version])]

        test_chunk_specs = []
        mock_get_chunk_specs.return_value = test_chunk_specs

        self._mapper.run(bundle_fqids)
        mock_lambda_invoke.assert_not_called()

        mock_expect_subtask_execution.assert_not_called()
        mock_complete_subtask_execution.assert_called_once_with(Subtask.MAPPER)

    def test_get_worker_payload(self):
        test_chunk_spec = []
        expected_payload = {
            'request_id': self.request_id,
            'request_hash': self.request_hash,
            'worker_chunk_spec': test_chunk_spec
        }

        payload = self._mapper._get_worker_payload(test_chunk_spec)

        self.assertTrue("request_hash" in payload)
        self.assertTrue("worker_chunk_spec" in payload)

        self.assertEqual(payload, expected_payload)

    @mock.patch("zarr.group")
    @mock.patch.object(DSSZarrStore, "__init__")
    def test_get_chunk_specs_ok(self, mock_dss_zarr_store, mock_zarr_group):
        bundle_uuid = str(uuid.uuid4())
        bundle_version = "version"
        bundle_fqids = ['.'.join([bundle_uuid, bundle_version])]
        chunk_size = 1
        nchunks = 1

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

    # TODO: Turn this back on once #95 is addressed
    @pytest.mark.skip(reason="Not needed while one-cell bundle assumption is in place.")
    @unittest.skip("Not needed while one-cell bundle assumption is in place.")
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

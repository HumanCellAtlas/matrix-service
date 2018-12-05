import hashlib
import os
import unittest
from unittest import mock

from matrix.common.exceptions import MatrixException
from matrix.common.zarr.dss_zarr_store import DSSZarrStore


class TestDSSZarrStore(unittest.TestCase):

    def setUp(self):
        self.bundle_uuid = "bundle1"
        self.bundle_version = "version1"

    @mock.patch("matrix.common.zarr.dss_zarr_store.DSSZarrStore.get_dss_client")
    def test_init_validate_ok(self, mock_get_dss_client):
        mock_get_dss_client.return_value = DSSClientStub()
        self.dss_zarr_store = DSSZarrStore(self.bundle_uuid, self.bundle_version)

    @mock.patch("matrix.common.zarr.dss_zarr_store.DSSZarrStore.get_dss_client")
    def test_init_validate_fail(self, mock_get_dss_client):
        mock_get_dss_client.return_value = DSSClientStub(use_valid_bundles=False)
        with self.assertRaises(MatrixException):
            self.dss_zarr_store = DSSZarrStore(self.bundle_uuid, self.bundle_version)

    @mock.patch("matrix.common.zarr.dss_zarr_store.DSSZarrStore.get_dss_client")
    def test_init_corrupted_files(self, mock_get_dss_client):
        mock_get_dss_client.return_value = DSSClientStub(use_valid_files=False)
        with self.assertRaises(RuntimeError):
            self.dss_zarr_store = DSSZarrStore(self.bundle_uuid, self.bundle_version)

    def test_get_dss_client(self):
        env_to_dss_host = {
            'predev': f"https://dss.integration.data.humancellatlas.org",
            'dev': f"https://dss.integration.data.humancellatlas.org",
            'integration': f"https://dss.integration.data.humancellatlas.org",
            'staging': f"https://dss.staging.data.humancellatlas.org",
            'prod': f"https://dss.data.humancellatlas.org",
        }

        for env in env_to_dss_host:
            os.environ['DEPLOYMENT_STAGE'] = env
            client = DSSZarrStore.get_dss_client()
            self.assertTrue(env_to_dss_host[env], client.host)


# Stub for hca.dss.DSSClient since DSSClient programmatically
# generates function signatures (e.g. get_bundle, get_file)
class DSSClientStub:
    TEST_FILE_CONTENTS = '{"zarr_format": 2}'.encode("utf-8")

    def __init__(self, use_valid_bundles=True, use_valid_files=True):
        """
        Create dummy responses for get_bundle and get_file
        """
        files_in_bundle = [
            ".zarr!.zgroup",
            ".zarr!cell_metadata_string!0.0",
            ".zarr!cell_metadata_string!.zarray",
            ".zarr!cell_metadata_numeric!0.0",
            ".zarr!cell_metadata_numeric!.zarray",
            ".zarr!cell_metadata_numeric_name!0",
            ".zarr!cell_metadata_numeric_name!.zarray",
            ".zarr!cell_metadata_string_name!0",
            ".zarr!cell_metadata_string_name!.zarray",
            ".zarr!gene_id!.zarray",
            ".zarr!cell_id!.zarray",
            ".zarr!expression!.zarray",
            ".zarr!expression!0.0",
        ]
        if not use_valid_bundles:
            files_in_bundle = files_in_bundle[:-2]

        self.bundle_response = {
            'bundle': {
                'files': []
            },
            'version': "version1",
            'creator_uid': "",
            'uuid': "bundle1"
        }
        for filename in files_in_bundle:
            self.bundle_response['bundle']['files'].append({
                'sha1': hashlib.sha1(DSSClientStub.TEST_FILE_CONTENTS).hexdigest() if use_valid_files else "test_sha",
                'name': filename,
                'uuid': "test_uuid",
                'crc32c': "test_crc32c",
                'version': "test_version",
                'indexed': True,
                's3_etag': "test_s3_etag",
                'sha256': "test_sha256",
                'content-type': "test_content_type",
                'size': len(DSSClientStub.TEST_FILE_CONTENTS)
            })

        # Stubs dcp-cli:hca.util._ClientMethodFactory
        self.get_file = DSSClientStub.Streamable()

    def get_bundle(self, uuid, version, replica):
        return self.bundle_response

    class Streamable:
        def stream(self, uuid, version, replica):
            return DSSClientStub.StreamableResponse()

    class StreamableResponse:
        def __init__(self):
            self.raw = DSSClientStub.RawResponse()

        def __enter__(self, **kwargs):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return self

    class RawResponse:
        def read(self):
            return DSSClientStub.TEST_FILE_CONTENTS

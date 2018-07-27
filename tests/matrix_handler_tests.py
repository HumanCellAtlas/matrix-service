import os
import tempfile
import traceback
import unittest

from hca.util import SwaggerAPIException
from chalicelib import rand_uuid
from chalicelib.request_handler import RequestHandler
from cloud_blobstore import BlobNotFoundError
from chalicelib.config import MERGED_MTX_BUCKET_NAME, s3_blob_store
from chalicelib.matrix_handler import LoomMatrixHandler
from tests import get_random_existing_bundle_uuids, rand_uuids


class TestMatrixHandler(unittest.TestCase):
    def test_download_mtx(self):
        """
        Make sure that mtx file paths returned from _download_mtx() exist.
        """
        # Get a random subset of bundle_uuids from sample bundle uuids
        bundle_uuids = get_random_existing_bundle_uuids(ub=5)

        # Download mtx from bundles
        mtx_handler = LoomMatrixHandler()

        with tempfile.TemporaryDirectory() as temp_dir:
            mtx_paths = mtx_handler._download_mtx(bundle_uuids, temp_dir)

            # Check whether the downloaded matrices actually exist.
            for mtx_path in mtx_paths:
                self.assertTrue(os.path.exists(mtx_path))

        self.assertFalse(os.path.exists(temp_dir))

    def test_download_mtx_exception(self):
        """
        Make sure download_mtx() with non-existing bundle_uuids raise SwaggerAPIException.
        """
        invalid_bundle_uuids = rand_uuids(ub=5)
        mtx_handler = LoomMatrixHandler()

        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertRaises(SwaggerAPIException, mtx_handler._download_mtx, invalid_bundle_uuids, temp_dir)

        self.assertFalse(os.path.exists(temp_dir))

    def test_concat_mtx(self):
        """
        Make sure concat_mtx() generates a valid merge_mtx path.
        """
        # Get a random subset of bundle_uuids from sample bundle uuids
        bundle_uuids = get_random_existing_bundle_uuids(ub=5)
        request_id = RequestHandler.generate_request_id(bundle_uuids)

        # Download mtx from bundles
        mtx_handler = LoomMatrixHandler()

        with tempfile.TemporaryDirectory() as temp_dir:
            mtx_paths = mtx_handler._download_mtx(bundle_uuids, temp_dir)
            _, merged_mtx_path = tempfile.mkstemp(dir=temp_dir, suffix=".loom")

            # Merge matrices
            mtx_handler._concat_mtx(mtx_paths, merged_mtx_path)

            self.assertTrue(os.path.exists(merged_mtx_path))

        self.assertFalse(os.path.exists(temp_dir))

    def test_upload_mtx(self):
        """
        Make sure that _upload_mtx() can successfully upload the merged matrix,
        and delete the local merged matrix file after uploading it.
        """

        with tempfile.TemporaryDirectory() as temp_dir:
            key = rand_uuid()
            _, path = tempfile.mkstemp(dir=temp_dir)
            mtx_handler = LoomMatrixHandler()
            mtx_handler._upload_mtx(path, key)

        self.assertFalse(os.path.exists(temp_dir))

        try:
            s3_blob_store.get(bucket=MERGED_MTX_BUCKET_NAME, key=key + ".loom")
        except BlobNotFoundError:
            error_msg = traceback.format_exc()
            self.fail(error_msg)

        # Delete the key after checking
        s3_blob_store.delete(bucket=MERGED_MTX_BUCKET_NAME, key=key + ".loom")


if __name__ == '__main__':
    unittest.main()

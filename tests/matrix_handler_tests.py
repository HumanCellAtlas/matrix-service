import os
import shutil
import tempfile
import traceback
import unittest

from cloud_blobstore import BlobNotFoundError
from chalicelib.config import MERGED_MTX_BUCKET_NAME, s3_blob_store
from chalicelib.matrix_handler import LoomMatrixHandler
from tests import get_random_existing_bundle_uuids


class TestMatrixHandler(unittest.TestCase):
    def test_download_mtx(self):
        """
        Make sure that mtx file paths returned from _download_mtx() exist.
        """
        # Get a random subset of bundle_uuids from sample bundle uuids
        bundle_uuids_subset = get_random_existing_bundle_uuids(ub=5)

        # Download mtx from bundles
        mtx_handler = LoomMatrixHandler()
        mtx_dir, mtx_paths = mtx_handler._download_mtx(bundle_uuids_subset)

        # Check whether the downloaded matrices actually exist.
        for mtx_path in mtx_paths:
            self.assertTrue(os.path.exists(mtx_path))

        # Remove all created temp files
        shutil.rmtree(mtx_dir)

    def test_upload_mtx(self):
        """
        Make sure that _upload_mtx() can successfully upload the merged matrix,
        and delete the local merged matrix file after uploading it.
        """
        temp_dir = tempfile.mkdtemp()
        _, path = tempfile.mkstemp(dir=temp_dir)
        mtx_handler = LoomMatrixHandler()
        key = mtx_handler._upload_mtx(path)

        self.assertFalse(os.path.exists(path))

        try:
            s3_blob_store.get(bucket=MERGED_MTX_BUCKET_NAME, key=key)
        except BlobNotFoundError:
            error_msg = traceback.format_exc()
            self.fail(error_msg)

        # Delete the key after checking
        s3_blob_store.delete(bucket=MERGED_MTX_BUCKET_NAME, key=key)


if __name__ == '__main__':
    unittest.main()

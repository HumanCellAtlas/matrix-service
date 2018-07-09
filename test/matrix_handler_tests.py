import shutil
import unittest

from chalicelib import *
from chalicelib.constants import MERGED_MTX_BUCKET_NAME, REQUEST_STATUS_BUCKET_NAME
from chalicelib.matrix_handler import LoomMatrixHandler
from chalicelib.request_handler import RequestHandler


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
        self.assertTrue(s3key_exists(key, MERGED_MTX_BUCKET_NAME))

        # Delete the key after checking
        delete_s3key(key=key, bucket_name=MERGED_MTX_BUCKET_NAME)

    def test_run_merge_request(self):
        """
        Make sure that run_merge_request() can successfully create a merged matrix and
        a request status file in s3. (Assumption: Matrices concatenation always works
        correctly.)
        """
        # Get a random subset of bundle_uuids from sample bundle uuids
        bundle_uuids_subset = get_random_existing_bundle_uuids(ub=5)

        # Generate a request id based on bundle uuids
        request_id = RequestHandler.generate_request_id(bundle_uuids_subset)

        # Run merge request
        mtx_handler = LoomMatrixHandler()
        mtx_handler.run_merge_request(bundle_uuids=bundle_uuids_subset, request_id=request_id)

        merged_mtx_key = request_id + ".loom"
        request_status_key = request_id + ".json"

        self.assertTrue(s3key_exists(key=merged_mtx_key, bucket_name=MERGED_MTX_BUCKET_NAME))
        self.assertTrue(s3key_exists(key=request_status_key, bucket_name=REQUEST_STATUS_BUCKET_NAME))

        # Delete keys after checking
        delete_s3key(key=merged_mtx_key, bucket_name=MERGED_MTX_BUCKET_NAME)
        delete_s3key(key=request_status_key, bucket_name=REQUEST_STATUS_BUCKET_NAME)


if __name__ == '__main__':
    unittest.main()

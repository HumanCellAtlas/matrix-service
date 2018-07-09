import shutil
import unittest
import hca

from chalicelib import *
from chalicelib.constants import STAGING_BUCKET_NAME, MERGED_MTX_BUCKET_NAME
from chalicelib.matrix_handler import LoomMatrixHandler


class TestMatrixHandler(unittest.TestCase):
    def test_filter_mtx(self):
        """
        Make sure _filter_mtx() always filters correct number of mtx from
        DSS bundles.
        """
        # Generate a random number of temp directories containing some
        # random files
        temp_dirs = mk_rand_dirs(5, 10)

        # Get the number of ".loom" matrix files within the directories
        local_mtx_num = scan_dirs(temp_dirs, ".loom")

        client = hca.dss.DSSClient()
        bundle_uuids = []

        # Upload all temp directories into DSS as bundles
        try:
            for temp_dir in temp_dirs:
                response = client.upload(
                    src_dir=temp_dir,
                    replica="aws",
                    staging_bucket=STAGING_BUCKET_NAME
                )
                bundle_uuids.append(response["bundle_uuid"])

            mtx_handler = LoomMatrixHandler()
            remote_mtx_num = len(mtx_handler._filter_mtx(bundle_uuids))
            self.assertEqual(local_mtx_num, remote_mtx_num)
        finally:
            for temp_dir in temp_dirs:
                shutil.rmtree(temp_dir)

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

        delete_s3key(key=key, bucket_name=MERGED_MTX_BUCKET_NAME)


if __name__ == '__main__':
    unittest.main()

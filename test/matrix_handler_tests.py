import json
import os
import random
import shutil
import unittest
import hca

from chalicelib import mk_temp_dirs, scan_dirs
from chalicelib.constants import STAGING_BUCKET_NAME, BUNDLE_UUIDS_PATH
from chalicelib.matrix_handler import LoomMatrixHandler


class TestMatrixHandler(unittest.TestCase):
    # def test_filter_mtx(self):
    #     """
    #     Make sure _filter_mtx() always filters correct number of mtx from
    #     DSS bundles.
    #     """
    #
    #     # Generate a random number of temp directories
    #     temp_dirs = mk_temp_dirs()
    #
    #     # Get the number of ".loom" matrix files within the directories
    #     local_mtx_num = scan_dirs(temp_dirs, ".loom")
    #
    #     client = hca.dss.DSSClient()
    #     bundle_uuids = []
    #
    #     # Upload all temp directories into DSS as bundles
    #     for temp_dir in temp_dirs:
    #         response = client.upload(
    #             src_dir=temp_dir,
    #             replica="aws",
    #             staging_bucket=STAGING_BUCKET_NAME
    #         )
    #         bundle_uuids.append(response["bundle_uuid"])
    #
    #     mtx_handler = LoomMatrixHandler()
    #     remote_mtx_num = len(mtx_handler._filter_mtx(bundle_uuids))
    #     self.assertEqual(local_mtx_num, remote_mtx_num)

    def test_download_mtx(self):
        """
        Make sure that mtx file paths returned from _download_mtx() exist.
        """
        # Get a random subset of bundle_uuids from sample bundle uuids
        with open(BUNDLE_UUIDS_PATH, "r") as f:
            sample_bundle_uuids = json.loads(f.read())

        n = random.randint(1, 5)
        bundle_uuids_subset = random.sample(sample_bundle_uuids, n)

        # Download mtx from bundles
        mtx_handler = LoomMatrixHandler()
        mtx_paths = mtx_handler._download_mtx(bundle_uuids_subset)

        self.assertTrue(len(mtx_paths) > 0)

        dir_path = os.path.dirname(mtx_paths[0])

        # Check whether the downloaded matrices actually exist.
        for mtx_path in mtx_paths:
            self.assertTrue(os.path.exists(mtx_path))

        # Remove all created temp files
        shutil.rmtree(dir_path)


if __name__ == '__main__':
    unittest.main()

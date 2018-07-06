import os
import unittest
import hca

from chalicelib import mk_temp_dirs, scan_dirs
from chalicelib.constants import STAGING_BUCKET_NAME
from chalicelib.matrix_handler import LoomMatrixHandler


class TestMatrixHandler(unittest.TestCase):
    def test_filter_mtx(self):
        """
        Test the logic for filtering only ".loom" matrices file within bundles
        """

        # Generate a random number of temp directories
        temp_dirs = mk_temp_dirs()

        # Get the number of ".loom" matrix files within the directories
        local_mtx_num = scan_dirs(temp_dirs, ".loom")

        client = hca.dss.DSSClient()
        bundle_uuids = []

        # Upload all temp directories into DSS as bundles
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


if __name__ == '__main__':
    unittest.main()

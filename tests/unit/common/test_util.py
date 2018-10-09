import numpy
import unittest
import pytest

from matrix.common.util import get_client, convert_zarr_store_to_pandas_df
from matrix.common.hca_store import HCAStore
from .. import test_bundle_spec


class TestUtils(unittest.TestCase):

    def test_convert_zarr_store_to_pandas_df(self):
        """Test the HCA matrix wrapper"""

        dss_client = get_client()
        bundle_uuid = test_bundle_spec["uuid"]
        bundle_version = test_bundle_spec["version"]

        zarr_store = HCAStore(dss_client=dss_client,
                              bundle_uuid=bundle_uuid,
                              bundle_version=bundle_version)

        exp_df, meta_df = convert_zarr_store_to_pandas_df(zarr_store)

        assert numpy.sum(exp_df.values) == pytest.approx(test_bundle_spec["description"]["sums"]["expression"], 1)
        assert numpy.sum(meta_df.values) == pytest.approx(test_bundle_spec["description"]["sums"]["cell_metadata"], 1)

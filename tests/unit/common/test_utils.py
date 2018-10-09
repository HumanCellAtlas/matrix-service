import numpy
import unittest
import pytest

from matrix.common.utils import convert_zarr_store_to_pandas_df
from matrix.common.dss_zarr_store import DSSZarrStore
from .. import test_bundle_spec


class TestUtils(unittest.TestCase):

    def test_convert_zarr_store_to_pandas_df(self):
        """Test the HCA matrix wrapper"""

        bundle_uuid = test_bundle_spec["uuid"]
        bundle_version = test_bundle_spec["version"]

        zarr_store = DSSZarrStore(bundle_uuid=bundle_uuid,
                                  bundle_version=bundle_version)

        exp_df, meta_df = convert_zarr_store_to_pandas_df(zarr_store)

        assert numpy.sum(exp_df.values) == pytest.approx(test_bundle_spec["description"]["sums"]["expression"], 1)
        assert numpy.sum(meta_df.values) == pytest.approx(test_bundle_spec["description"]["sums"]["cell_metadata"], 1)

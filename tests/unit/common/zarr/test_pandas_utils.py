import numpy
import unittest
import pytest

import zarr

from matrix.common.zarr.pandas_utils import convert_dss_zarr_root_to_subset_pandas_dfs
from matrix.common.zarr.pandas_utils import apply_filter_to_matrix_pandas_dfs
from matrix.common.zarr.dss_zarr_store import DSSZarrStore
from tests import test_bundle_spec


class TestPandasUtils(unittest.TestCase):

    def setUp(self):
        bundle_uuid = test_bundle_spec["uuid"]
        bundle_version = test_bundle_spec["version"]
        dss_zarr_store = DSSZarrStore(bundle_uuid=bundle_uuid, bundle_version=bundle_version)
        self.dss_zarr_root = zarr.group(store=dss_zarr_store)
        self.exp_df, self.meta_df = convert_dss_zarr_root_to_subset_pandas_dfs(self.dss_zarr_root, 0, 1)

    def _test_convert_dss_zarr_root_to_subset_pandas_dfs(self):
        expected_exp_df_values_sum = pytest.approx(test_bundle_spec["description"]["sums"]["expression"], 1)
        self.assertEqual(numpy.sum(self.exp_df.values), expected_exp_df_values_sum)

        expected_meta_df_values_sum = pytest.approx(test_bundle_spec["description"]["sums"]["cell_metadata_numeric"], 1)
        self.assertEqual(numpy.sum(self.meta_df.select_dtypes("float32").values), expected_meta_df_values_sum)

    def _test_apply_filter_to_matrix_pandas_dfs_all_results(self):
        filter_string = "ALIGNED_READS>1.0"
        filtered_exp_df, filtered_meta_df = apply_filter_to_matrix_pandas_dfs(filter_string, self.exp_df, self.meta_df)
        expected_exp_df_values_sum = pytest.approx(test_bundle_spec["description"]["sums"]["expression"], 1)
        self.assertEqual(numpy.sum(filtered_exp_df.values), expected_exp_df_values_sum)
        expected_meta_df_values_sum = pytest.approx(test_bundle_spec["description"]["sums"]["cell_metadata_numeric"], 1)
        self.assertEqual(numpy.sum(filtered_meta_df.select_dtypes("float32").values), expected_meta_df_values_sum)

    def _test_apply_filter_to_matrix_pandas_dfs_no_results(self):
        filter_string = "ALIGNED_READS>100000000000.0"
        filtered_exp_df, filtered_meta_df = apply_filter_to_matrix_pandas_dfs(filter_string, self.exp_df, self.meta_df)
        self.assertEqual(numpy.sum(filtered_exp_df.values), 0.0)
        self.assertEqual(numpy.sum(filtered_meta_df.select_dtypes("float32").values), 0.0)

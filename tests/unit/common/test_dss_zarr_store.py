import binascii
import unittest

import numpy
import pytest
import zarr

from matrix.common.dss_zarr_store import DSSZarrStore
from .. import test_bundle_spec


class TestDSSZarrStore(unittest.TestCase):

    def test_hca_store_read(self):
        """Test using the DSSZarrStore with zarr."""

        bundle_uuid = test_bundle_spec["uuid"]
        bundle_version = test_bundle_spec["version"]
        replica = test_bundle_spec["replica"]
        expected_values = test_bundle_spec["description"]

        hca_store = DSSZarrStore(bundle_uuid=bundle_uuid, bundle_version=bundle_version, replica=replica)
        matrix_root = zarr.group(store=hca_store)

        for dset, expected_shape in expected_values.get("shapes", {}).items():
            assert getattr(matrix_root, dset).shape == expected_shape

        for dset, expected_sum in expected_values.get("sums", {}).items():
            assert numpy.sum(getattr(matrix_root, dset)) == pytest.approx(expected_sum, 1)

        for dset, expected_digest in expected_values.get("digests", {}).items():
            assert binascii.hexlify(getattr(matrix_root, dset).digest()) == expected_digest
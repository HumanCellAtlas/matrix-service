import binascii
import unittest

import hca
import numpy
import pytest
import zarr

from matrix.common.hca_store import HCAStore
from .. import test_bundle_spec

TEST_DSS_HOST = 'https://dss.integration.data.humancellatlas.org/v1'


class TestLambdaHandler(unittest.TestCase):

    def test_hca_store_read(self):
        """Test using the HCAStore with zarr."""

        bundle_uuid = test_bundle_spec["uuid"]
        bundle_version = test_bundle_spec["version"]
        replica = test_bundle_spec["replica"]
        expected_values = test_bundle_spec["description"]

        client = hca.dss.DSSClient()
        client.host = TEST_DSS_HOST

        hca_store = HCAStore(client, bundle_uuid, bundle_version, replica)
        matrix_root = zarr.group(store=hca_store)

        for dset, expected_shape in expected_values.get("shapes", {}).items():
            assert getattr(matrix_root, dset).shape == expected_shape

        for dset, expected_sum in expected_values.get("sums", {}).items():
            assert numpy.sum(getattr(matrix_root, dset)) == pytest.approx(expected_sum, 1)

        for dset, expected_digest in expected_values.get("digests", {}).items():
            assert binascii.hexlify(getattr(matrix_root, dset).digest()) == expected_digest

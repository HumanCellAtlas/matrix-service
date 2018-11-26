import os
import unittest

from matrix.common.zarr.dss_zarr_store import DSSZarrStore


class TestDSSZarrStore(unittest.TestCase):

    def test_get_dss_client(self):
        env_to_dss_host = {
            'predev': f"https://dss.integration.data.humancellatlas.org",
            'dev': f"https://dss.integration.data.humancellatlas.org",
            'integration': f"https://dss.integration.data.humancellatlas.org",
            'staging': f"https://dss.staging.data.humancellatlas.org",
            'prod': f"https://dss.data.humancellatlas.org",
        }

        for env in env_to_dss_host:
            os.environ['DEPLOYMENT_STAGE'] = env
            client = DSSZarrStore.get_dss_client()
            self.assertTrue(env_to_dss_host[env], client.host)

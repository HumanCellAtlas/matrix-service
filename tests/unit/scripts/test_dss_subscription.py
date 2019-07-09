import mock
import os
import unittest

from scripts.dss_subscription import recreate_dss_subscription


class TestDssSubscription(unittest.TestCase):

    def setUp(self):
        self.swagger_spec_stub = {
            'info': {
                'description': "test_description"
            },
            'host': "test_host",
            'basePath': "/v1",
            'paths': {}
        }

    @mock.patch("scripts.dss_subscription.put_subscription")
    @mock.patch("scripts.dss_subscription.delete_subscription")
    @mock.patch("scripts.dss_subscription.get_subscriptions")
    @mock.patch("hca.dss.DSSClient.swagger_spec", new_callable=mock.PropertyMock)
    @mock.patch("scripts.dss_subscription.retrieve_gcp_credentials")
    def test_recreate_dss_subscription(self,
                                       mock_retrieve_gcp_credentials,
                                       mock_swagger_spec,
                                       mock_get_subscriptions,
                                       mock_delete_subscription,
                                       mock_put_subscription):
        callback = f"https://matrix.{os.environ['DEPLOYMENT_STAGE']}.data.humancellatlas.org/v0/dss/notifications"
        mock_retrieve_gcp_credentials.return_value = {}
        mock_swagger_spec.return_value = self.swagger_spec_stub
        mock_get_subscriptions.return_value = [{'uuid': "test_uuid", 'callback_url': callback}]

        recreate_dss_subscription()

        mock_get_subscriptions.assert_called_once_with(dss_client=mock.ANY,
                                                       replica="aws",
                                                       subscription_type="jmespath")
        mock_delete_subscription.assert_called_once_with(dss_client=mock.ANY,
                                                         replica="aws",
                                                         subscription_type="jmespath",
                                                         uuid="test_uuid")
        # mock_put_subscription.assert_called_once_with(dss_client=mock.ANY,
        #                                               callback_url=callback,
        #                                               jmespath_query=mock.ANY,
        #                                               replica="aws")

    class MatrixInfraConfigStub:
        def __init__(self):
            self.gcp_service_acct_creds = "{}".encode()

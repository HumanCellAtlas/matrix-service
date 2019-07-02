import concurrent.futures
import os
import unittest
from unittest import mock

from matrix.lambdas.daemons.notification import NotificationHandler


class TestNotificationHandler(unittest.TestCase):

    def setUp(self):
        self.bundle_uuid = "test_uuid"
        self.bundle_version = "test_version"

    @mock.patch("matrix.lambdas.daemons.notification.NotificationHandler.update_bundle")
    def test_create_event(self, mock_update_bundle):
        handler = NotificationHandler(self.bundle_uuid, self.bundle_version, "CREATE")
        handler.run()

        mock_update_bundle.assert_called_once_with()

    @mock.patch("matrix.lambdas.daemons.notification.NotificationHandler.update_bundle")
    def test_update_event(self, mock_update_bundle):
        handler = NotificationHandler(self.bundle_uuid, self.bundle_version, "UPDATE")
        handler.run()

        mock_update_bundle.assert_called_once_with()

    @mock.patch("matrix.lambdas.daemons.notification.NotificationHandler.remove_bundle")
    def test_delete_event(self, mock_remove_bundle):
        handler = NotificationHandler(self.bundle_uuid, self.bundle_version, "DELETE")
        handler.run()

        mock_remove_bundle.assert_called_once_with()

    @mock.patch("matrix.lambdas.daemons.notification.NotificationHandler.remove_bundle")
    def test_tombstone_event(self, mock_remove_bundle):
        handler = NotificationHandler(self.bundle_uuid, self.bundle_version, "TOMBSTONE")
        handler.run()

        mock_remove_bundle.assert_called_once_with()

    @mock.patch("matrix.lambdas.daemons.notification.logger.error")
    @mock.patch("matrix.lambdas.daemons.notification.NotificationHandler.remove_bundle")
    @mock.patch("matrix.lambdas.daemons.notification.NotificationHandler.update_bundle")
    def test_invalid_event(self, mock_update_bundle, mock_remove_bundle, mock_error):
        handler = NotificationHandler(self.bundle_uuid, self.bundle_version, "INVALID")
        handler.run()

        self.assertFalse(mock_update_bundle.called)
        self.assertFalse(mock_remove_bundle.called)
        self.assertTrue(mock_error.called)

    @mock.patch("shutil.rmtree")
    @mock.patch("matrix.common.etl.etl_dss_bundles")
    def test_update_bundle(self, mock_etl_dss_bundles, mock_rmtree):
        handler = NotificationHandler(self.bundle_uuid, self.bundle_version, "CREATE")
        handler.update_bundle()
        query = {
            "query": {
                "bool": {
                    "must": [{"term": {"uuid": self.bundle_uuid}}]
                }
            }
        }

        mock_rmtree.assert_called_once_with("/tmp/output", ignore_errors=True)
        mock_etl_dss_bundles.assert_called_once_with(query=query,
                                                     content_type_patterns=mock.ANY,
                                                     filename_patterns=mock.ANY,
                                                     transformer_cb=mock.ANY,
                                                     finalizer_cb=mock.ANY,
                                                     staging_directory="/tmp",
                                                     deployment_stage=os.environ['DEPLOYMENT_STAGE'],
                                                     max_workers=mock.ANY,
                                                     dispatcher_executor_class=concurrent.futures.ThreadPoolExecutor)

    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    def test_remove_bundle(self, mock_transaction):
        handler = NotificationHandler(self.bundle_uuid, self.bundle_version, "TOMBSTONE")
        handler.remove_bundle()

        mock_transaction.assert_called_once_with([
            NotificationHandler.DELETE_EXPRESSION_QUERY_TEMPLATE.format(
                bundle_uuid=self.bundle_uuid,
                bundle_version=self.bundle_version
            ),
            NotificationHandler.DELETE_CELL_QUERY_TEMPLATE.format(
                bundle_uuid=self.bundle_uuid,
                bundle_version=self.bundle_version
            ),
            NotificationHandler.DELETE_ANALYSIS_QUERY_TEMPLATE.format(
                bundle_uuid=self.bundle_uuid,
                bundle_version=self.bundle_version
            )
        ])

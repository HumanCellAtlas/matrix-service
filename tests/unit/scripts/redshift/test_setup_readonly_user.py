import mock
import unittest

from scripts.redshift.setup_readonly_user import handler


class TestSetupReadonlyUser(unittest.TestCase):

    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    @mock.patch("scripts.redshift.setup_readonly_user.retrieve_redshift_config")
    def test_handler(self, mock_retrieve_redshift_config, mock_transaction):
        mock_retrieve_redshift_config.return_value = TestSetupReadonlyUser.MatrixRedshiftConfigStub()
        handler()

        expected_calls = [
            mock.call(["DROP USER IF EXISTS test_username;",
                      "CREATE USER test_username WITH PASSWORD 'test_password';"]),
            mock.call(["grant select on all tables in schema public to test_username;",
                      "grant select on all tables in schema information_schema to test_username;"])
        ]
        mock_transaction.assert_has_calls(expected_calls)

    class MatrixRedshiftConfigStub:
        def __init__(self):
            self.readonly_username = "test_username"
            self.readonly_password = "test_password"

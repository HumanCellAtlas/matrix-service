import os
from unittest import mock

from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, DeploymentTableField
from matrix.common.exceptions import MatrixException
from scripts.redshift.bump_data_version import bump_data_version
from tests.unit import MatrixTestCaseUsingMockAWS


class TestBumpDataVersion(MatrixTestCaseUsingMockAWS):
    def setUp(self):
        super(TestBumpDataVersion, self).setUp()

        self.create_test_data_version_table()
        self.create_test_deployment_table()
        self.create_test_request_table()

        self.init_test_data_version_table()
        self.init_test_deployment_table()

        self.dynamo_handler = DynamoHandler()
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']

    @mock.patch("matrix.common.v1_api_handler.V1ApiHandler.describe_filter")
    def test_bump_data_version(self, mock_describe_filter):
        self.assertEqual(self._get_current_data_version(), 0)
        self.assertTrue(self._data_version_exists(0))
        self.assertFalse(self._data_version_exists(1))

        mock_describe_filter.return_value = {'cell_counts': {'test_project': 1}}
        bump_data_version()

        self.assertEqual(self._get_current_data_version(), 1)
        self.assertTrue(self._data_version_exists(0))
        self.assertTrue(self._data_version_exists(1))

    @mock.patch("matrix.common.v1_api_handler.V1ApiHandler.describe_filter")
    def test_bump_data_version_existing(self, mock_describe_filter):
        mock_describe_filter.return_value = {'cell_counts': {'test_project': 1}}
        bump_data_version()
        self.assertEqual(self._get_current_data_version(), 1)
        self.assertTrue(self._data_version_exists(1))

        with mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_data_version_table_entry") as \
                mock_create_data_version_table_entry:
            self._set_data_version(0)
            self.assertEqual(self._get_current_data_version(), 0)
            bump_data_version()
            mock_create_data_version_table_entry.assert_not_called()
            self.assertEqual(self._get_current_data_version(), 1)

    def _set_data_version(self, version):
        self.dynamo_handler.set_table_field_with_value(table=DynamoTable.DEPLOYMENT_TABLE,
                                                       key=self.deployment_stage,
                                                       field_enum=DeploymentTableField.CURRENT_DATA_VERSION,
                                                       field_value=version)

    def _data_version_exists(self, version) -> bool:
        try:
            self.dynamo_handler.get_table_item(DynamoTable.DATA_VERSION_TABLE, version)
            return True
        except MatrixException:
            return False

    def _get_current_data_version(self) -> int:
        deployment_entry = self.dynamo_handler.get_table_item(DynamoTable.DEPLOYMENT_TABLE, self.deployment_stage)
        current_data_version = deployment_entry[DeploymentTableField.CURRENT_DATA_VERSION.value]

        return current_data_version

import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from matrix.common.exceptions import MatrixException
from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, DeploymentTableField


def bump_data_version():
    """
    Increment a deployment's current data version in the Deployment table in DynamoDb.
    If the new version does not exist in the Data Version table, generate a new one based on the current deployment.
    """
    dynamo_handler = DynamoHandler()
    deployment_stage = os.environ['DEPLOYMENT_STAGE']

    current_data_version = \
        dynamo_handler.get_table_item(table=DynamoTable.DEPLOYMENT_TABLE,
                                      key=deployment_stage)[DeploymentTableField.CURRENT_DATA_VERSION.value]
    new_data_version = current_data_version + 1

    try:
        dynamo_handler.get_table_item(table=DynamoTable.DATA_VERSION_TABLE,
                                      key=new_data_version)
    except MatrixException:
        dynamo_handler.create_data_version_table_entry(new_data_version)

    dynamo_handler.set_table_field_with_value(table=DynamoTable.DEPLOYMENT_TABLE,
                                              key=deployment_stage,
                                              field_enum=DeploymentTableField.CURRENT_DATA_VERSION,
                                              field_value=new_data_version)


if __name__ == '__main__':  # pragma: no cover
    bump_data_version()

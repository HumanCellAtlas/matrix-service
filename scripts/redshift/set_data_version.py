import argparse
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from matrix.common.exceptions import MatrixException
from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, DeploymentTableField
from matrix.common.logging import Logging

logger = Logging.get_logger(__file__)


def set_data_version(version):
    """
    Set a deployment's current data version in the Deployment table in DynamoDb.
    If the desired version does not exist in the Data Version table, the request will fail.
    """
    dynamo_handler = DynamoHandler()
    deployment_stage = os.environ['DEPLOYMENT_STAGE']

    try:
        dynamo_handler.get_table_item(table=DynamoTable.DATA_VERSION_TABLE,
                                      key=version)
        dynamo_handler.set_table_field_with_value(table=DynamoTable.DEPLOYMENT_TABLE,
                                                  key=deployment_stage,
                                                  field_enum=DeploymentTableField.CURRENT_DATA_VERSION,
                                                  field_value=version)
    except MatrixException:
        logger.error(f"Version {version} does not exist in {DynamoTable.DEPLOYMENT_TABLE.value}. "
                     f"Please use an existing version or generate one via `make bump-data-version.`")
        exit(1)


if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--version",
                        help="Version number to set",
                        type=int)
    args = parser.parse_args()

    set_data_version(args.version)

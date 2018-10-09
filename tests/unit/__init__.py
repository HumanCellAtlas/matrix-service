import os
import unittest

from moto import mock_dynamodb2

# TODO: set DEPLOYMENT_STAGE=test when test env exists
os.environ['DEPLOYMENT_STAGE'] = "dev"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"
os.environ['AWS_ACCESS_KEY_ID'] = "ak"
os.environ['AWS_SECRET_ACCESS_KEY'] = "sk"
os.environ['LAMBDA_DRIVER_FUNCTION_NAME'] = f"dcp-matrix-service-driver-{os.environ['DEPLOYMENT_STAGE']}"
os.environ['DYNAMO_STATE_TABLE_NAME'] = f"dcp-matrix-service-state-table-{os.environ['DEPLOYMENT_STAGE']}"

test_bundle_spec = {
    "uuid": "680a9934-63ab-4fc7-a9a9-50ccc332f871",
    "version": "2018-09-20T211624.579399Z",
    "replica": "aws",
    "description": {
        "shapes": {
            "cell_id": (1,),
            "cell_metadata": (1, 4),
            "cell_metadata_name": (4,),
            "expression": (1, 58347),
            "gene_id": (58347,),
            "gene_metadata": (0,),
            "gene_metadata_name": (0,),
        },
        "sums": {
            "expression": 1000000,
            "cell_metadata": 10896283,
        },
        "digests": {
            "cell_id": b'11a8effc57c8db3e6247264f1f41e1c80dee00a2',
            "cell_metadata": b'3680e7a6162b9e2afda25ba6fed71d79323bd263',
            "cell_metadata_name": b'feade7edbf56c82df1031039431e667477cc6eba',
            "expression": b'047d15818efb1ebc3de83d7f9ea9d11fe3c3c619',
            "gene_id": b'ff6bc94b0205c5118bdd984c70da5e4f76b84cbc'
        }
    }
}


class MatrixTestCaseUsingMockAWS(unittest.TestCase):
    def setUp(self):
        self.dynamo_mock = mock_dynamodb2()
        self.dynamo_mock.start()

    def tearDown(self):
        self.dynamo_mock.stop()

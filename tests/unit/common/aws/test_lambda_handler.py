import unittest
import uuid

from botocore.stub import Stubber

from matrix.common.aws.lambda_handler import LambdaHandler
from matrix.common.aws.lambda_handler import LambdaName


class TestLambdaHandler(unittest.TestCase):
    """
    Environment variables are set in tests/unit/__init__.py
    """
    def setUp(self):
        self.request_id = str(uuid.uuid4())
        self.handler = LambdaHandler()
        self.mock_lambda_client = Stubber(self.handler._client)

    def test_invoke(self):
        expected_params = {'FunctionName': LambdaName.DRIVER_V0.value,
                           'InvocationType': "Event",
                           'Payload': b"{}"}
        self.mock_lambda_client.add_response('invoke', {}, expected_params)
        self.mock_lambda_client.activate()
        self.handler.invoke(LambdaName.DRIVER_V0, {})

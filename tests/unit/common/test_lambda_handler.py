import unittest

from botocore.stub import Stubber

from matrix.common.lambda_handler import LambdaHandler
from matrix.common.lambda_handler import LambdaName


class TestLambdaHandler(unittest.TestCase):
    """
    Environment variables are set in tests/__init__.py
    """
    def setUp(self):
        self.handler = LambdaHandler()
        self.mock_handler = Stubber(self.handler._client)

    def test_invoke(self):
        expected_params = {'FunctionName': LambdaName.DRIVER.value,
                           'InvocationType': "Event",
                           'Payload': b"{}"}
        self.mock_handler.add_response('invoke', {}, expected_params)
        self.mock_handler.activate()
        self.handler.invoke(LambdaName.DRIVER, {})

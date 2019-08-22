import unittest
from unittest import mock

from matrix.common.v1_api_handler import V1ApiHandler


class TestV1ApiHandler(unittest.TestCase):
    def setUp(self):
        self.api_handler = V1ApiHandler()

    @mock.patch("requests.get")
    def test_describe_filter(self, mock_get):
        mock_get.return_value = TestV1ApiHandler.StubResponse()

        self.api_handler.describe_filter("test_filter")
        mock_get.assert_called_once_with(f"{self.api_handler.api_url}/filters/test_filter")

    class StubResponse:
        def __init__(self):
            self.content = b"{}"

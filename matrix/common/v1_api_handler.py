import json
import os

import requests


class V1ApiHandler:
    """
    Class for interacting with the V1 Matrix API.
    """
    def __init__(self):
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']

        if self.deployment_stage == "prod":
            self.api_url = f"https://matrix.data.humancellatlas.org/v1"
        else:
            self.api_url = f"https://matrix.{self.deployment_stage}.data.humancellatlas.org/v1"

    def describe_filter(self, filter_: str) -> dict:
        """
        Hits the /filters/<filter> endpoint to introspect a specific filter and its indexed data in Redshift.
        :param filter_: str Filter to introspect
        :return: dict /filters/<filter> response
        """
        url = f"{self.api_url}/filters/{filter_}"
        response = requests.get(url)
        return json.loads(response.content.decode())

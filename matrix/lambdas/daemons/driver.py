import typing

from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.lambda_handler import LambdaHandler
from matrix.common.lambda_handler import LambdaName


class Driver:
    """
    The first task in a distributed filter merge job.
    """
    def __init__(self):
        self.lambda_handler = LambdaHandler()
        self.dynamo_handler = DynamoHandler()

    def run(self, request_id: str, bundle_fqids: typing.List[str], format: str):
        """
        Initialize a filter merge job and spawn a mapper task for each bundle_fqid.

        :param request_id: Filter merge job request ID
        :param bundle_fqids: List of bundle fqids to be queried on
        :param format: MatrixFormat file format of expression matrices
        """
        self.dynamo_handler.create_state_table_entry(request_id, len(bundle_fqids))
        self.dynamo_handler.create_output_table_entry(request_id)

        for fqid in bundle_fqids:
            mapper_payload = {
                'request_id': request_id,
                'bundle_fqid': fqid,
                'format': format,
            }

            self.lambda_handler.invoke(LambdaName.MAPPER, mapper_payload)

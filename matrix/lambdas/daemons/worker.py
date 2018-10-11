from matrix.common.lambda_handler import LambdaHandler
from matrix.common.dynamo_handler import DynamoHandler


class Worker:
    """
    The worker (third) task in a distributed filter merge job.
    """
    def __init__(self):
        self.lambda_handler = LambdaHandler()
        self.dynamo_handler = DynamoHandler()

    def run(self, request_id: str, filter_string: str, worker_chunk_spec: dict):
        """
        Filter one work chunk and invoke reducer lambda when last worker job is completed.

        :param request_id: Filter merge job request ID
        :param filter string: user provided filter string to filter out results
        :worker_chunk_spec: dict with keys 'bundle_uuid', 'bundle_version', 'start_row', 'num_rows'
        """
        pass

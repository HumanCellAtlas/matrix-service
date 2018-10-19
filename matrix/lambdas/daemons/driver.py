import itertools
import math
import typing

from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.lambda_handler import LambdaHandler
from matrix.common.lambda_handler import LambdaName


class Driver:
    """
    The first task in a distributed filter merge job.
    """
    def __init__(self, bundles_per_worker=100):
        self.lambda_handler = LambdaHandler()
        self.dynamo_handler = DynamoHandler()
        self._bundles_per_worker = bundles_per_worker

    def run(self, request_id: str, bundle_fqids: typing.List[str], format: str):
        """
        Initialize a filter merge job and spawn a mapper task for each bundle_fqid.

        :param request_id: Filter merge job request ID
        :param bundle_fqids: List of bundle fqids to be queried on
        :param format: MatrixFormat file format of expression matrices
        """
        num_expected_mappers = math.ceil(len(bundle_fqids) / self._bundles_per_worker)
        self.dynamo_handler.create_state_table_entry(request_id, num_expected_mappers)
        self.dynamo_handler.create_output_table_entry(request_id)

        def _group_bundles(bundle_fqids, bundles_per_group):
            """Split the list of bundle_fqids into lists with at most bundles_per_group
            items.
            """
            iter_args = [iter(bundle_fqids)] * self._bundles_per_worker
            for bundle_fqid_group in itertools.zip_longest(*iter_args):
                yield list(filter(None, bundle_fqid_group))

        for bundle_fqid_group in _group_bundles(bundle_fqids, self._bundles_per_worker):
            mapper_payload = {
                'request_id': request_id,
                'bundle_fqids': bundle_fqids,
                'format': format,
            }
            self.lambda_handler.invoke(LambdaName.MAPPER, mapper_payload)

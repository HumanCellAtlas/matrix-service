import itertools
import math
import typing

from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.lambda_handler import LambdaHandler, LambdaName
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class Driver:
    """
    The first task in a distributed filter merge job.
    """
    def __init__(self, request_id: str, bundles_per_worker: int=100):
        Logging.set_correlation_id(logger, value=request_id)

        self.request_id = request_id
        self.bundles_per_worker = bundles_per_worker

        self.lambda_handler = LambdaHandler()
        self.dynamo_handler = DynamoHandler()

    def run(self, bundle_fqids: typing.List[str], format: str):
        """
        Initialize a filter merge job and spawn a mapper task for each bundle_fqid.

        :param bundle_fqids: List of bundle fqids to be queried on
        :param format: MatrixFormat file format of output expression matrix
        """
        logger.debug(f"Driver running with parameters: bundle_fqids={bundle_fqids}, "
                     f"format={format}, bundles_per_worker={self.bundles_per_worker}")

        num_expected_mappers = int(math.ceil(len(bundle_fqids) / self.bundles_per_worker))
        self.dynamo_handler.create_state_table_entry(self.request_id, num_expected_mappers)
        self.dynamo_handler.create_output_table_entry(self.request_id)

        logger.debug(f"Invoking {num_expected_mappers} Mapper(s) with approximately "
                     f"{self.bundles_per_worker} bundles per Mapper.")

        for bundle_fqid_group in self._group_bundles(bundle_fqids, self.bundles_per_worker):
            mapper_payload = {
                'request_id': self.request_id,
                'bundle_fqids': bundle_fqid_group,
                'format': format,
            }
            self.lambda_handler.invoke(LambdaName.MAPPER, mapper_payload)

    @staticmethod
    def _group_bundles(bundle_fqids, bundles_per_group):
        """Split the list of bundle_fqids into lists with at most bundles_per_group
        items.
        """
        iter_args = [iter(bundle_fqids)] * bundles_per_group
        for bundle_fqid_group in itertools.zip_longest(*iter_args):
            yield list(filter(None, bundle_fqid_group))

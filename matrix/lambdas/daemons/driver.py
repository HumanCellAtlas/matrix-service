import itertools
import math
import typing

import requests

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

    def run(self, bundle_fqids: typing.List[str], bundle_fqids_url: str, format: str):
        """
        Initialize a filter merge job and spawn a mapper task for each bundle_fqid.

        :param bundle_fqids: List of bundle fqids to be queried on
        :param bundle_fqids_url: URL from which bundle_fqids can be retrieved
        :param format: MatrixFormat file format of output expression matrix
        """
        logger.debug(f"Driver running with parameters: bundle_fqids={bundle_fqids}, "
                     f"bundle_fqids_url={bundle_fqids_url}, format={format}, "
                     f"bundles_per_worker={self.bundles_per_worker}")

        if bundle_fqids_url:
            data = requests.get(bundle_fqids_url)
            resolved_bundle_fqids = self._parse_download_manifest(data)
        else:
            resolved_bundle_fqids = bundle_fqids

        num_expected_mappers = int(math.ceil(len(resolved_bundle_fqids) / self.bundles_per_worker))
        self.dynamo_handler.create_state_table_entry(self.request_id, num_expected_mappers, format)
        self.dynamo_handler.create_output_table_entry(self.request_id, format)

        logger.debug(f"Invoking {num_expected_mappers} Mapper(s) with approximately "
                     f"{self.bundles_per_worker} bundles per Mapper.")

        for bundle_fqid_group in self._group_bundles(resolved_bundle_fqids, self.bundles_per_worker):
            mapper_payload = {
                'request_id': self.request_id,
                'bundle_fqids': bundle_fqid_group,
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

    @staticmethod
    def _parse_download_manifest(data: str) -> typing.List[str]:
        def _parse_line(line: str) -> str:
            tokens = line.split("\t")
            return f"{tokens[0]}.{tokens[1]}"

        lines = data.splitlines()[1:]
        return list(map(_parse_line, lines))

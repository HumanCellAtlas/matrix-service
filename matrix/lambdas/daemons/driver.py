import itertools
import math
import typing

import requests

from matrix.common.lambda_handler import LambdaHandler, LambdaName
from matrix.common.logging import Logging
from matrix.common.request_tracker import RequestTracker, Subtask

logger = Logging.get_logger(__name__)


class Driver:
    """
    The first task in a distributed filter merge job.
    """
    def __init__(self, request_id: str, bundles_per_worker: int=100):
        Logging.set_correlation_id(logger, value=request_id)

        self.request_id = request_id
        self.bundles_per_worker = bundles_per_worker

        self.request_tracker = RequestTracker(self.request_id)
        self.lambda_handler = LambdaHandler(self.request_id)

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
            response = requests.get(bundle_fqids_url)
            resolved_bundle_fqids = self._parse_download_manifest(response.text)
        else:
            resolved_bundle_fqids = bundle_fqids
        logger.debug(f"resolved bundles: {resolved_bundle_fqids}")

        num_expected_mappers = int(math.ceil(len(resolved_bundle_fqids) / self.bundles_per_worker))
        self.request_tracker.init_request(num_expected_mappers, format)

        logger.debug(f"Invoking {num_expected_mappers} Mapper(s) with approximately "
                     f"{self.bundles_per_worker} bundles per Mapper.")

        for bundle_fqid_group in self._group_bundles(resolved_bundle_fqids, self.bundles_per_worker):
            mapper_payload = {
                'request_id': self.request_id,
                'bundle_fqids': bundle_fqid_group,
            }
            logger.debug(f"Invoking {LambdaName.MAPPER} with {mapper_payload}")
            self.lambda_handler.invoke(LambdaName.MAPPER, mapper_payload)

        self.request_tracker.complete_subtask_execution(Subtask.DRIVER)

    @staticmethod
    def _group_bundles(bundle_fqids, bundles_per_group):
        """Split the list of bundle_fqids into lists with at most bundles_per_group
        items.
        """
        iter_args = [iter(bundle_fqids)] * bundles_per_group
        logger.debug(f"iter_args: {iter_args}")
        for bundle_fqid_group in itertools.zip_longest(*iter_args):
            logger.debug("Yielding")
            yield list(filter(None, bundle_fqid_group))

    @staticmethod
    def _parse_download_manifest(data: str) -> typing.List[str]:
        def _parse_line(line: str) -> str:
            tokens = line.split("\t")
            return f"{tokens[0]}.{tokens[1]}"

        lines = data.splitlines()[1:]
        return list(map(_parse_line, lines))

import itertools
import math
import typing

import requests

from matrix.common.lambda_handler import LambdaHandler, LambdaName
from matrix.common.logging import Logging
from matrix.common.request_cache import RequestCache
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

        self.lambda_handler = LambdaHandler()

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

        request_hash = RequestCache(self.request_id).set_hash(resolved_bundle_fqids, format)
        logger.debug(f"Calculated and set hash {request_hash} for request {self.request_id}")
        request_tracker = RequestTracker(request_hash)

        if request_tracker.is_initialized:
            if not request_tracker.error:
                logger.debug(f"Halting because {request_hash} already exists and has not "
                             f"(yet) failed.")
                return

        logger.debug("Request hash not found, so starting the whole show.")

        num_expected_mappers = int(math.ceil(len(resolved_bundle_fqids) / self.bundles_per_worker))
        request_tracker.initialize_request(num_expected_mappers, format)

        logger.debug(f"Invoking {num_expected_mappers} Mapper(s) with approximately "
                     f"{self.bundles_per_worker} bundles per Mapper.")

        for bundle_fqid_group in self._group_bundles(resolved_bundle_fqids, self.bundles_per_worker):
            mapper_payload = {
                'request_hash': request_hash,
                'bundle_fqids': bundle_fqid_group,
            }
            logger.debug(f"Invoking {LambdaName.MAPPER} with {mapper_payload}")
            self.lambda_handler.invoke(LambdaName.MAPPER, mapper_payload)

        request_tracker.complete_subtask_execution(Subtask.DRIVER)

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

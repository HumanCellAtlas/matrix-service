import typing

from matrix.common.dynamo_handler import DynamoHandler, DynamoTable, StateTableField
from matrix.common.lambda_handler import LambdaHandler, LambdaName
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class Mapper:
    """
    The second task in a distributed filter merge job and responsible for parallelizing
    the work of filtering a single bundle.
    Mapper takes a single bundle (uuid, version) as input, reads the associated expression matrix
    from the DSS, and invokes a Worker task for each chunk (row subset) of the expression matrix.
    """
    def __init__(self, request_id: str):
        Logging.set_correlation_id(logger, value=request_id)

        self.request_id = request_id

        self.lambda_handler = LambdaHandler()
        self.dynamo_handler = DynamoHandler()

    def run(self, bundle_fqids: typing.List[str]):
        """
        Mapper entry point.
        Invokes one Worker lambda for every chunk of the input expression matrix.
        Updates relevant fields in the State tracking DynamoDB table.

        :param bundle_fqids: List of fully-qualified IDs of the analysis bundles containing the
            expression matrix to be filtered
        :return:
        """
        logger.debug(f"Mapper running with parameters: bundle_fqids={bundle_fqids}")

        worker_chunk_specs = Mapper._get_chunk_specs(bundle_fqids)

        if worker_chunk_specs:
            self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                      self.request_id,  # TODO: init DH with this to remove from params
                                                      StateTableField.EXPECTED_WORKER_EXECUTIONS,
                                                      1)
            logger.debug(f"Invoking 1 worker lambda with {len(worker_chunk_specs)} chunks.")
            self.lambda_handler.invoke(LambdaName.WORKER, self._get_worker_payload(worker_chunk_specs))
            logger.debug(f"Worker invoked {worker_chunk_specs}")

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_MAPPER_EXECUTIONS,
                                                  1)

    def _get_worker_payload(self, worker_chunk_spec: typing.List[dict]) -> dict:
        """
        Builds the data payload to invoke a Worker lambda.

        :param worker_chunk_spec: Data specific to each Worker, created by _get_chunk_specs
        :return: Worker lambda data payload
        """
        return {
            'request_id': self.request_id,
            'worker_chunk_spec': worker_chunk_spec
        }

    @staticmethod
    def _get_chunk_specs(bundle_fqids: typing.List[str]) -> typing.List[dict]:
        """
        Retrieves an expression matrix from a bundle in the DSS,
        parses out and returns chunking information about the matrix.

        :param bundle_uuid: Bundle UUID of the analysis bundle containing the expression matrix
        :param bundle_version: Bundle version of the analysis bundle containing the expression matrix
        :return: List of dicts describing row subsets (chunks) of the input expression matrix
        """

        chunk_work_spec = []
        for bundle_fqid in bundle_fqids:
            bundle_uuid, bundle_version = bundle_fqid.split(".", 1)
            # TODO: Not this! This is taking advantage of the fact that every
            # matrixable bundle in the DSS will at first have data for a single
            # cell. That won't be true for too long

            chunk_work_spec.extend(
                [{"bundle_uuid": bundle_uuid,
                  "bundle_version": bundle_version,
                  "start_row": 0,
                  "num_rows": 1}])
        return chunk_work_spec

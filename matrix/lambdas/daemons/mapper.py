import typing

import zarr

from matrix.common.dss_zarr_store import DSSZarrStore
from matrix.common.dynamo_handler import DynamoTable, StateTableField
from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.lambda_handler import LambdaHandler
from matrix.common.lambda_handler import LambdaName


class Mapper:
    """
    The second task in a distributed filter merge job and responsible for parallelizing
    the work of filtering a single bundle.
    Mapper takes a single bundle (uuid, version) as input, reads the associated expression matrix
    from the DSS, and invokes a Worker task for each chunk (row subset) of the expression matrix.
    """
    def __init__(self, request_id: str, format: str):
        print(f"Mapper initialized with: {request_id}, {format}")
        self.request_id = request_id
        self.format = format

        self.lambda_handler = LambdaHandler()
        self.dynamo_handler = DynamoHandler()

    def run(self, bundle_uuid: str, bundle_version: str):
        """
        Mapper entry point.
        Invokes one Worker lambda for every chunk of the input expression matrix.
        Updates relevant fields in the State tracking DynamoDB table.

        :param bundle_uuid: Bundle UUID of the analysis bundle containing the expression matrix to be filtered
        :param bundle_version: Bundle version of the analysis bundle containing the expression matrix to be filtered
        :return:
        """
        print(f"Mapper running with: {bundle_uuid}, {bundle_version}")
        worker_chunk_specs = Mapper._get_chunk_specs(bundle_uuid, bundle_version)

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,  # TODO: init DH with this to remove from params
                                                  StateTableField.EXPECTED_WORKER_EXECUTIONS.value,  # TODO: use enum
                                                  len(worker_chunk_specs))

        for worker_chunk_spec in worker_chunk_specs:
            self.lambda_handler.invoke(LambdaName.WORKER, self._get_worker_payload(worker_chunk_spec))

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_MAPPER_EXECUTIONS.value,
                                                  1)

    def _get_worker_payload(self, worker_chunk_spec: dict) -> dict:
        """
        Builds the data payload to invoke a Worker lambda.

        :param worker_chunk_spec: Data specific to each Worker, created by _get_chunk_specs
        :return: Worker lambda data payload
        """
        return {
            'request_id': self.request_id,
            'format': self.format,
            'worker_chunk_spec': worker_chunk_spec
        }

    @staticmethod
    def _get_chunk_specs(bundle_uuid: str, bundle_version: str) -> typing.List[dict]:
        """
        Retrieves an expression matrix from a bundle in the DSS,
        parses out and returns chunking information about the matrix.

        :param bundle_uuid: Bundle UUID of the analysis bundle containing the expression matrix
        :param bundle_version: Bundle version of the analysis bundle containing the expression matrix
        :return: List of dicts describing row subsets (chunks) of the input expression matrix
        """
        zarr_store = DSSZarrStore(bundle_uuid, bundle_version=bundle_version, dss_instance="dev")
        root = zarr.group(store=zarr_store)

        rows_per_chunk = root.expression.chunks[0]
        total_chunks = root.expression.nchunks

        return [{"bundle_uuid": bundle_uuid,
                 "bundle_version": bundle_version,
                 "start_row": n * rows_per_chunk,
                 "num_rows": rows_per_chunk}
                for n in range(total_chunks)]

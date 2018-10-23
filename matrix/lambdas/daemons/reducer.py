from matrix.common.dynamo_handler import DynamoHandler, DynamoTable, StateTableField
from matrix.common.s3_zarr_store import S3ZarrStore
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class Reducer:
    def __init__(self, request_id: str, format: str):
        Logging.set_correlation_id(logger, value=request_id)

        self.request_id = request_id
        self.format = format

        self.dynamo_handler = DynamoHandler()

    def run(self):
        """
        Write resultant expression matrix zarr metadata in S3 after Workers complete.
        """
        logger.debug(f"Reducer running with parameters: format={self.format}")

        s3_zarr_store = S3ZarrStore(self.request_id)
        s3_zarr_store.write_group_metadata()

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS,
                                                  1)

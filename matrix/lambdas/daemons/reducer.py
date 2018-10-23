from matrix.common.dynamo_handler import DynamoHandler
from matrix.common.dynamo_handler import DynamoTable
from matrix.common.dynamo_handler import StateTableField
from matrix.common.s3_zarr_store import S3ZarrStore


class Reducer:
    def __init__(self, request_id: str, format: str):
        print(f"Reducer created: {request_id}, {format}")
        self.request_id = request_id
        self.format = format

        self.dynamo_handler = DynamoHandler()

    def run(self):
        """
        Write resultant expression matrix zarr metadata in S3 after Workers complete.
        """
        print("Running reducer")
        s3_zarr_store = S3ZarrStore(self.request_id)
        s3_zarr_store.write_group_metadata()

        self.dynamo_handler.increment_table_field(DynamoTable.STATE_TABLE,
                                                  self.request_id,
                                                  StateTableField.COMPLETED_REDUCER_EXECUTIONS,
                                                  1)

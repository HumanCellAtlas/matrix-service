"""Script to pull from sqs and run redshift queries. Will be dockerized."""
import json
import os
from enum import Enum

from matrix.common.aws.batch_handler import BatchHandler
from matrix.common.aws.redshift_handler import RedshiftHandler
from matrix.common.aws.s3_handler import S3Handler
from matrix.common.aws.sqs_handler import SQSHandler
from matrix.common.config import MatrixInfraConfig
from matrix.common.logging import Logging
from matrix.common.request.request_tracker import RequestTracker, Subtask

logger = Logging.get_logger(__name__)


class QueryType(Enum):
    CELL = "cell"
    EXPRESSION = "expression"
    FEATURE = "feature"


class QueryRunner:

    def __init__(self):
        self.sqs_handler = SQSHandler()
        self.s3_handler = S3Handler(os.environ["MATRIX_QUERY_BUCKET"])
        self.batch_handler = BatchHandler()
        self.redshift_handler = RedshiftHandler()
        self.matrix_infra_config = MatrixInfraConfig()

    @property
    def query_job_q_url(self):
        return self.matrix_infra_config.query_job_q_url

    @property
    def query_job_deadletter_q_url(self):
        return self.matrix_infra_config.query_job_deadletter_q_url

    def check_cache(self):
        pass

    def run(self, max_loops=None):
        loops = 0
        while max_loops is None or loops < max_loops:
            loops += 1
            messages = self.sqs_handler.receive_messages_from_queue(self.query_job_q_url)
            if messages:
                message = messages[0]
                logger.info(f"Received {message} from {self.query_job_q_url}")
                payload = json.loads(message['Body'])
                request_id = payload['request_id']
                request_tracker = RequestTracker(request_id)
                Logging.set_correlation_id(logger, value=request_id)
                obj_key = payload['s3_obj_key']
                query_type = payload['type']
                receipt_handle = message['ReceiptHandle']
                try:
                    logger.info(f"Fetching query from {obj_key}")
                    query = self.s3_handler.load_content_from_obj_key(obj_key)

                    logger.info(f"Running query from {obj_key}")
                    self.redshift_handler.transaction([query], read_only=True)
                    logger.info(f"Finished running query from {obj_key}")

                    if query_type == QueryType.CELL:
                        self.check_cache()

                    logger.info(f"Deleting {message} from {self.query_job_q_url}")
                    self.sqs_handler.delete_message_from_queue(self.query_job_q_url, receipt_handle)

                    logger.info("Incrementing completed queries in state table")
                    request_tracker.complete_subtask_execution(Subtask.QUERY)

                    if request_tracker.is_request_ready_for_conversion():
                        logger.info("Scheduling batch conversion job")
                        batch_job_id = self.batch_handler.schedule_matrix_conversion(request_id, request_tracker.format)
                        request_tracker.write_batch_job_id_to_db(batch_job_id)
                except Exception as e:
                    logger.info(f"QueryRunner failed on {message} with error {e}")
                    request_tracker.log_error(str(e))
                    logger.info(f"Adding {message} to {self.query_job_deadletter_q_url}")
                    self.sqs_handler.add_message_to_queue(self.query_job_deadletter_q_url, payload)
                    logger.info(f"Deleting {message} from {self.query_job_q_url}")
                    self.sqs_handler.delete_message_from_queue(self.query_job_q_url, receipt_handle)
            else:
                logger.info(f"No messages to read from {self.query_job_q_url}")


def main():
    query_runner = QueryRunner()
    query_runner.run()


if __name__ == "__main__":
    print("STARTED query runner")
    main()

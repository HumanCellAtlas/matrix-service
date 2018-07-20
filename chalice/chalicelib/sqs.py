import json
import boto3

from typing import List
from chalicelib import rand_uuid
from chalicelib.config import MS_SQS_QUEUE_NAME, SQS_QUEUE_MSG, logger
from chalicelib.request_handler import RequestHandler, RequestStatus


class SqsQueueHandler:
    """
    AWS SQS Queue Handler for sending/receiving to/from message to
    matrix service's SQS queue.
    """

    # Get the matrix service sqs queue
    sqs = boto3.resource("sqs")
    ms_queue = sqs.get_queue_by_name(QueueName=MS_SQS_QUEUE_NAME)

    @staticmethod
    def send_msg_to_ms_queue(bundle_uuids: List[str], request_id: str) -> str:
        """
        Send a message to matrix service's sqs queue.
        :param bundle_uuids: UUID of the bundle.
        :param request_id: Concatenation request id.
        :return: Message ID of the msg being sent.
        """
        job_id = rand_uuid()

        logger.info("Request ID({}): Initialize the request with job id({})"
                    .format(request_id, job_id))

        RequestHandler.update_request(
            bundle_uuids=bundle_uuids,
            request_id=request_id,
            job_id=job_id,
            status=RequestStatus.INITIALIZED
        )

        # Create message to send to the SQS Queue
        msg = SQS_QUEUE_MSG.copy()
        msg["bundle_uuids"] = bundle_uuids
        msg["job_id"] = job_id

        logger.info("Request ID({}): Send request message({}) to SQS Queue."
                    .format(request_id, str(msg)))

        msg_str = json.dumps(msg, sort_keys=True)

        # Send the msg to the SQS queue
        response = SqsQueueHandler.ms_queue.send_message(MessageBody=msg_str)

        return response.get("MessageId")

    @staticmethod
    def msg_exists_ms_queue(msg_id: str) -> bool:
        """
        Check the existence of a msg in matrix service sqs queue.
        :param msg_id: Id of the message to check for existence.
        :return: True if msg exists in matrix service sqs queue.
        """
        for queue_msg in SqsQueueHandler.ms_queue.receive_messages():
            if queue_msg.message_id == msg_id:
                queue_msg.delete()
                return True

        return False

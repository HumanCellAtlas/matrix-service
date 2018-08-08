import json
import boto3

from chalicelib.config import MS_SQS_QUEUE_NAME


class SqsQueueHandler:
    """
    AWS SQS Queue Handler for sending/receiving to/from message to
    matrix service's SQS queue.
    """

    sqs = boto3.resource("sqs")

    @staticmethod
    def send_msg_to_ms_queue(payload: dict) -> str:
        """
        Send a message to matrix service's sqs queue.
        :param payload: the dict to serialize to json and send to SQS.
        :return: Message ID of the msg being sent.
        """
        # Create message to send to the SQS Queue
        msg_str = json.dumps(payload)

        # Send the msg to the SQS queue
        ms_queue = SqsQueueHandler.sqs.get_queue_by_name(QueueName=MS_SQS_QUEUE_NAME)
        response = ms_queue.send_message(MessageBody=msg_str)

        return response.get("MessageId")

    @staticmethod
    def msg_exists_ms_queue(msg_id: str) -> bool:
        """
        Check the existence of a msg in matrix service sqs queue.
        :param msg_id: Id of the message to check for existence.
        :return: True if msg exists in matrix service sqs queue.
        """
        ms_queue = SqsQueueHandler.sqs.get_queue_by_name(QueueName=MS_SQS_QUEUE_NAME)
        for queue_msg in ms_queue.receive_messages():
            if queue_msg.message_id == msg_id:
                queue_msg.delete()
                return True

        return False

import boto3

from chalicelib import generate_md5
from chalicelib.config import MS_SQS_QUEUE_NAME


class SqsQueueHandler:
    """
    AWS SQS Queue Handler for sending/receiving to/from message to
    matrix service's SQS queue.
    """

    # Get the matrix service sqs queue
    sqs = boto3.resource("sqs")
    ms_queue = sqs.get_queue_by_name(QueueName=MS_SQS_QUEUE_NAME)

    @staticmethod
    def send_msg_to_ms_queue(msg) -> str:
        """
        Send a message to matrix service's sqs queue.
        :param msg: Msg to send.
        :return: Message ID of the msg being sent.
        """
        response = SqsQueueHandler.ms_queue.send_message(MessageBody=msg)

        # Verify SQS received the message correctly
        msg_md5 = generate_md5(msg)
        assert msg_md5 == response.get("MD5OfMessageBody")

        return response.get("MessageId")

    @staticmethod
    def msg_exists_ms_queue(msg_id) -> bool:
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

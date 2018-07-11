import boto3

from chalicelib import generate_md5
from chalicelib.constants import MS_SQS_QUEUE_NAME


class SqsQueueHandler:
    """
    AWS SQS Queue Handler for sending/receiving to/from message to
    matrix service's SQS queue.
    """

    # Get the matrix service sqs queue
    sqs = boto3.resource("sqs")
    ms_queue = sqs.get_queue_by_name(QueueName=MS_SQS_QUEUE_NAME)

    @staticmethod
    def send_msg(msg):
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

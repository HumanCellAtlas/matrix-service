import json

import boto3
from tenacity import retry, wait_fixed, stop_after_attempt

from matrix.common.exceptions import MatrixException


class SQSHandler:
    """
    Interface for interacting with SQS.
    """

    def __init__(self):
        self.sqs = boto3.resource('sqs')

    @retry(reraise=True, wait=wait_fixed(2), stop=stop_after_attempt(5))
    def add_message_to_queue(self, queue_url: str, payload: dict):
        response = self.sqs.meta.client.send_message(QueueUrl=queue_url,
                                                     MessageBody=json.dumps(payload))
        status = response['ResponseMetadata']['HTTPStatusCode']
        if status != 200:
            raise MatrixException(status=500, title="Internal error",
                                  detail=f"Adding message for {payload} "
                                         f"was unsuccessful to SQS {queue_url} with status {status})")

    def receive_messages_from_queue(self, queue_url: str, wait_time=15, num_messages=1):
        response = self.sqs.meta.client.receive_message(QueueUrl=queue_url,
                                                        MaxNumberOfMessages=num_messages,
                                                        WaitTimeSeconds=wait_time)
        status = response['ResponseMetadata']['HTTPStatusCode']
        if status != 200:
            raise MatrixException(status=500, title="Internal error",
                                  detail=f"Retrieving message from {queue_url} "
                                         f"was unsuccessful with status {status})")
        return response.get('Messages')

    def delete_message_from_queue(self, queue_url: str, receipt_handle: str):
        response = self.sqs.meta.client.delete_message(QueueUrl=queue_url,
                                                       ReceiptHandle=receipt_handle)
        status = response['ResponseMetadata']['HTTPStatusCode']
        if status != 200:
            raise MatrixException(status=500, title="Internal error",
                                  detail=f"Deleting message with receipt handle {receipt_handle} from {queue_url} "
                                         f"was unsuccessful with status {status})")

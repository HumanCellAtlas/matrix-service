import json

from matrix.common.aws.sqs_handler import SQSHandler
from tests.unit import MatrixTestCaseUsingMockAWS


class TestSQSHandler(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestSQSHandler, self).setUp()
        self.sqs_handler = SQSHandler()
        self.sqs.meta.client.purge_queue(QueueUrl="test_query_job_q_name")

    def test_add_message_to_queue(self):
        payload = {'test_key': "test_value"}
        self.sqs_handler.add_message_to_queue("test_query_job_q_name", payload)

        messages = self.sqs.meta.client.receive_message(QueueUrl="test_query_job_q_name")
        message_body = json.loads(messages['Messages'][0]['Body'])
        self.assertEqual(message_body['test_key'], "test_value")

    def test_retrieve_messages_from_queue__returns_None_when_no_messages_found(self):
        message = self.sqs_handler.receive_messages_from_queue("test_query_job_q_name", 1)
        self.assertEqual(message, None)

    def test_retrieve_messages_from_queue__returns_message_when_message_is_found(self):
        payload = {'test_key': "test_value"}
        self.sqs_handler.add_message_to_queue("test_query_job_q_name", payload)

        messages = self.sqs_handler.receive_messages_from_queue(queue_url="test_query_job_q_name")

        message_body = json.loads(messages[0]['Body'])
        self.assertEqual(len(messages), 1)
        self.assertEqual(message_body['test_key'], "test_value")

    def test_delete_message_from_queue(self):
        payload = {'test_key': "test_value"}
        self.sqs_handler.add_message_to_queue("test_query_job_q_name", payload)
        messages = self.sqs_handler.receive_messages_from_queue(queue_url="test_query_job_q_name")
        receipt_handle = messages[0]['ReceiptHandle']

        self.sqs_handler.delete_message_from_queue("test_query_job_q_name", receipt_handle)

        message = self.sqs_handler.receive_messages_from_queue("test_query_job_q_name", 1)
        self.assertEqual(message, None)

import boto3
import os

from matrix.common.aws.dynamo_handler import LockTableField
from matrix.common.aws.dynamo_lock import DynamoLock
from tests.unit import MatrixTestCaseUsingMockAWS


class TestDynamoLock(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestDynamoLock, self).setUp()

        self.dynamo = boto3.resource("dynamodb", region_name=os.environ['AWS_DEFAULT_REGION'])

        self.key = "test_key"
        self.lock_table_name = os.environ['DYNAMO_LOCK_TABLE_NAME']
        self.create_test_lock_table()

    def test_context_manager_ok(self):
        with DynamoLock(self.key):
            item = self._query_lock_table(self.key)[0]
            self.assertEqual(item[LockTableField.LOCK_KEY.value], self.key)
            self.assertEqual(item[LockTableField.EXPIRATON_TIME.value], "no expiration")

        items = self._query_lock_table(self.key)
        self.assertTrue(len(items) == 0)

    def test_expired_lock(self):
        lock_1 = DynamoLock(self.key, 100)
        lock_1.acquire()
        lock_item_1 = self._query_lock_table(self.key)[0]

        lock_2 = DynamoLock(self.key, 100)
        lock_2.acquire()
        lock_item_2 = self._query_lock_table(self.key)[0]

        self.assertTrue(lock_item_1[LockTableField.EXPIRATON_TIME.value] <
                        lock_item_2[LockTableField.EXPIRATON_TIME.value])
        lock_2.release()

    def test_idempotent(self):
        lock = DynamoLock(self.key)
        lock.acquire()
        lock_item_1 = self._query_lock_table(self.key)[0]
        lock.acquire()
        lock_item_2 = self._query_lock_table(self.key)[0]

        self.assertEqual(lock_item_1, lock_item_2)

        lock.release()

    def _query_lock_table(self, key):
        response = self.dynamo.batch_get_item(
            RequestItems={
                self.lock_table_name: {
                    'Keys': [{'LockKey': key}]
                }
            }
        )
        return response['Responses'][self.lock_table_name]

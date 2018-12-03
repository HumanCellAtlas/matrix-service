"""
Implement a Lock object using DynamoDB. Can be used to acquire locks in distributed
environments like multiple independent lambdas.
"""
import datetime
import time
import uuid

import boto3
import botocore

from matrix.common.aws.dynamo_handler import DynamoTable, LockTableField
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class DynamoLock(object):
    """Implement a lock with DynamoDB."""

    timestamp_fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(self, lock_key, expiration_in_ms=None):
        """Init the lock object.
        Args:
          lock_key: String representing the resource associated with the lock. For
            example, if you're locking an S3 object, this would be the bucket/key
            for the object.
          expiration_in_ms: Optionally set an expiration time for the lock. This
            is helpful if the acquirer dies gracelessly, for example.
        """
        self._lock_key = lock_key
        self._expiration_in_ms = expiration_in_ms
        self._lock_id = str(uuid.uuid4())
        self._lock_table_name = DynamoTable.LOCK_TABLE.value

    def __enter__(self):
        """Support `with DynamoLock(...)`"""
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()

    def _get_boto3_table(self):
        """Return the boto3 Table object for the lock table."""
        return boto3.resource("dynamodb").Table(self._lock_table_name)

    def _expiration_time(self):
        """Return a timestamp for the expiration of a lock."""
        if not self._expiration_in_ms:
            return "no expiration"

        return (datetime.datetime.utcnow() +
                datetime.timedelta(milliseconds=self._expiration_in_ms)).strftime(
                    self.timestamp_fmt)

    def _has_expired(self, expiration_timestamp):

        if expiration_timestamp == "no expiration":
            return False

        expiration_time = datetime.datetime.strptime(
            expiration_timestamp, self.timestamp_fmt)

        return expiration_time < datetime.datetime.utcnow()

    def acquire(self):
        """Acquire the lock."""

        lock_table = self._get_boto3_table()

        while True:

            db_response = lock_table.get_item(
                Key={"LockKey": self._lock_key},
                ConsistentRead=True
            )

            # If the lock key doesn't exist, then we're free to try to acquire
            # the lock.
            if "Item" not in db_response:
                try:
                    lock_table.put_item(
                        Item={
                            LockTableField.LOCK_KEY.value: self._lock_key,
                            LockTableField.LOCK_HOLDER.value: self._lock_id,
                            LockTableField.EXPIRATON_TIME.value: self._expiration_time()

                        },
                        ConditionExpression="attribute_not_exists(LockKey)"
                    )
                    return
                # If this didn't work, someone else got the lock first.
                except botocore.exceptions.ClientError as exc:
                    if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        pass
                    else:
                        raise
            # If the lock has expired, we can also try to acquire it.
            elif self._has_expired(db_response["Item"][LockTableField.EXPIRATON_TIME.value]):
                try:
                    lock_table.update_item(
                        Key={LockTableField.LOCK_KEY.value: self._lock_key},
                        UpdateExpression=f"SET {LockTableField.LOCK_KEY.value} = :n, "
                                         f"{LockTableField.EXPIRATON_TIME.value} = :e",
                        ConditionExpression=f"{LockTableField.LOCK_HOLDER.value} = :f",
                        ExpressionAttributeValues={":n": self._lock_id,
                                                   ":e": self._expiration_time(),
                                                   ":f": db_response["Item"][LockTableField.LOCK_HOLDER.value]}
                    )
                    return
                except botocore.exceptions.ClientError as exc:
                    if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        pass
                    else:
                        raise
            # Or maybe we hold the lock ourselves, then just return
            elif db_response["Item"][LockTableField.LOCK_HOLDER.value] == self._lock_id:
                return
            logger.debug(f"Waiting for lock on {self._lock_key}")
            # Chill out for a bit
            time.sleep(6)

    def release(self):
        """Release the lock.
        If it turns out we don't hold the lock, raise ClientError.
        """
        lock_table = self._get_boto3_table()

        lock_table.delete_item(
            Key={LockTableField.LOCK_KEY.value: self._lock_key},
            ConditionExpression=f"{LockTableField.LOCK_HOLDER.value} = :i",
            ExpressionAttributeValues={":i": self._lock_id}
        )

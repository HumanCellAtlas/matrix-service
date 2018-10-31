import copy
import hashlib
import random
import uuid

from matrix.common.request_cache import RequestCache, RequestNotFound
from tests.unit import MatrixTestCaseUsingMockAWS


class TestRequestCache(MatrixTestCaseUsingMockAWS):

    def setUp(self):
        super(TestRequestCache, self).setUp()

        self.request_id = str(uuid.uuid4())
        self.request_hash = hashlib.sha256().hexdigest()
        self.request_cache = RequestCache(self.request_id)

        self.create_test_cache_table()

    def test_uninitialized_request(self):

        with self.assertRaises(RequestNotFound):
            self.request_cache.retrieve_hash()

    def test_set_and_retrieve_hash(self):

        bundle_fqids = ["bundle1.version0", "bundle2.version0"]
        format_ = "test_format"

        hash_1 = self.request_cache.set_hash(bundle_fqids, format_)

        copy_bundle_fqids = copy.deepcopy(bundle_fqids)
        random.shuffle(copy_bundle_fqids)
        hash_2 = RequestCache(str(uuid.uuid4())).set_hash(copy_bundle_fqids, format_)
        hash_3 = RequestCache(str(uuid.uuid4())).set_hash(bundle_fqids, format_ + '_')

        self.assertEqual(hash_1, hash_2)
        self.assertNotEqual(hash_2, hash_3)

        retrieved_hash = self.request_cache.retrieve_hash()
        self.assertEqual(retrieved_hash, hash_1)

    def test_initialize(self):
        self.request_cache.initialize()
        self.assertIsNone(self.request_cache.retrieve_hash())

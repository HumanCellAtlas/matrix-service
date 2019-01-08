import datetime
import unittest

from matrix.common import date


class TestDate(unittest.TestCase):

    def setUp(self):
        self.now = datetime.datetime.utcnow()

    def test_get_datetime_now(self):
        date_now = date.get_datetime_now()
        self.assertTrue((date_now - self.now).total_seconds() <= 1)

    def test_conversions(self):
        self.assertEqual(date.to_datetime(date.to_string(self.now)), self.now)

import shutil
from zkv.zkv import ZKV, ZKVError
from zkv.backend import MemoryBackend

import unittest
import os

pjoin = os.path.join

class TestZKV(unittest.TestCase):
    def setUp(self):
        self.conn = ZKV(MemoryBackend()).connect("pool")

    def test_get_set_item(self):
        self.conn[b"foobar"] = b"example"

        self.assertEqual(self.conn[b"foobar"].read(), b"example")

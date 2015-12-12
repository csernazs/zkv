import shutil
from zkv.backend import HashedDirectory, MemoryBackend

import unittest
import tempfile
import os

pjoin = os.path.join

class TestHashedDirectory(unittest.TestCase):
    POOL = "pool1"
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(suffix=".zkv")
        self.pool_dir = pjoin(self.temp_dir, self.POOL)
        os.mkdir(pjoin(self.pool_dir))

    def test_read(self):
        pool_dir = self.pool_dir
        os.mkdir(pjoin(pool_dir, "88"))
        with open(pjoin(pool_dir, "88", "8843d7f92416211de9ebb963ff4ce28125932878"), "wb") as outfile:
            outfile.write(b"abcde")

        hd = HashedDirectory(self.temp_dir).connect(self.POOL)
        value = hd.get(b"foobar")
        self.assertEqual(value.read(2), b"ab")
        value.seek(0)
        self.assertEqual(value.read(), b"abcde")
        with self.assertRaises(KeyError):
            hd.get(b"no_such_key")

    def test_create(self):
        pool_dir = self.pool_dir
        path = pjoin(pool_dir, "88", "8843d7f92416211de9ebb963ff4ce28125932878")
        hd = HashedDirectory(self.temp_dir).connect(self.POOL)
        value = hd.create(b"foobar")
        value.write("abcde")
        value.flush()
        data = open(path, "rb").read()
        self.assertEqual(data, b"abcde")
        value.write("fgh")
        value.close()
        data = open(path, "rb").read()
        self.assertEqual(data, b"abcdefgh")

    def test_ioerror_file(self):
        pool_dir = self.pool_dir
        path = pjoin(pool_dir, "88", "8843d7f92416211de9ebb963ff4ce28125932878")
        os.makedirs(path)
        hd = HashedDirectory(self.temp_dir).connect(self.POOL)
        with self.assertRaises(IOError):
            hd.get(b"foobar")

    def test_ioerror_directory(self):
        pool_dir = self.pool_dir

        path = pjoin(pool_dir, "88", "8843d7f92416211de9ebb963ff4ce28125932878")
        os.chmod(pool_dir, 0o444)
        hd = HashedDirectory(self.temp_dir).connect(self.POOL)
        with self.assertRaises(OSError):
            hd.create(b"foobar")


    def test_delete(self):
        pool_dir = self.pool_dir
        path = pjoin(pool_dir, "88", "8843d7f92416211de9ebb963ff4ce28125932878")
        hd = HashedDirectory(self.temp_dir).connect(self.POOL)
        value = hd.create(b"foobar")
        value.write(b"example")
        value.close()

        self.assertEqual(open(path, "rb").read(), b"example")

        hd.delete(b"foobar")

        self.assertFalse(os.path.isfile(path), msg="File must not exist after delete")


    def tearDown(self):
        shutil.rmtree(self.pool_dir, ignore_errors=False)


class BackendTest:
    pool = None
    def test_create(self):
        value = self.pool.create(b"foobar")
        value.write(b"example")
        value.flush()
        self.assertEqual(self.pool.get(b"foobar").read(), b"example")
        value.write(b" test")
        value.close()
        self.assertEqual(self.pool.get(b"foobar").read(), b"example test")

    def test_create_overwrite(self):
        value = self.pool.create(b"foobar")
        value.write(b"example")
        value.close()

        value = self.pool.create(b"foobar")
        value.write(b"example2")
        value.close()

        self.assertEqual(self.pool.get(b"foobar").read(), b"example2")

    def test_get(self):
        value = self.pool.create(b"foobar")
        value.write(b"example")
        value.close()

        self.assertEqual(self.pool.get(b"foobar").read(), b"example")

    def test_get_and_write(self):
        self.pool.create(b"foobar").close()
        new = self.pool.get(b"foobar")
        new.write(b"example")
        new.close()

        self.assertEqual(self.pool.get(b"foobar").read(), b"example")


    def test_get_exception(self):
        with self.assertRaises(KeyError):
            self.pool.delete(b"foobar")

    def test_race_overwrite(self):
        value1 = self.pool.create(b"foobar")
        value1.write("test")

        value2 = self.pool.get(b"foobar")
        value2.write("test2")
        value2.close()

        self.assertEqual(self.pool.get(b"foobar").read(), b"test2")


    def test_delete_exception(self):
        with self.assertRaises(KeyError):
            self.pool.delete(b"foobar")

    def test_iterkeys(self):
        self.assertEqual(list(self.pool.iterkeys()), [])

        self.pool.create(b"foobar")
        self.assertEqual(list(self.pool.iterkeys()), [b"foobar"])

        self.pool.create(b"foobaz")
        self.assertEqual(set(self.pool.iterkeys()), {b"foobaz", b"foobar"})

    def test_contains(self):
        self.assertFalse(self.pool.contains(b"foobar"), msg="Key must not exist")

        self.pool.create(b"foobar")
        self.assertTrue(self.pool.contains(b"foobar"), msg="Key must exist")


class HashedDirectoryBackendTest(BackendTest, unittest.TestCase):
    POOL = "pool1"

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(suffix=".zkv")
        self.pool_dir = pjoin(self.temp_dir, self.POOL)
        os.mkdir(pjoin(self.pool_dir))

        self.pool = HashedDirectory(self.temp_dir).connect(self.POOL)

    def tearDown(self):
        shutil.rmtree(self.pool_dir, ignore_errors=False)


class MemoryBackendTest(BackendTest, unittest.TestCase):
    def setUp(self):
        self.pool = MemoryBackend().connect("pool")

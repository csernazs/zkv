
import hashlib
import os
from .exc import Error
import errno
from filesystem import PosixFilesystem
import io

pjoin = os.path.join


class Backend:
    def connect(self, pool):
        raise NotImplementedError

class Pool:
    pass

class HashedDirectoryError(Error):
    pass


class Proxy:
    pass


class FileProxy(Proxy):
    def __init__(self, file_object):
        self.file_object = file_object
        
    def read(self, count=None):
        if count is None:
            return self.file_object.read()
        else:
            return self.file_object.read(count)
    
    def write(self, data):
        return self.file_object.write(data)
    
    def seek(self, offset, whence=0):
        return self.file_object.seek(offset, whence)
    
    def close(self):
        return self.file_object.close()
    
    def flush(self):
        return self.file_object.flush()


class HashedDirectory:
    def __init__(self, base_dir, file_mode=0o644, dir_mode=0o755, hash_size=2, key_algo="sha1", filesystem=PosixFilesystem()):
        self.base_dir = base_dir
        self.file_mode = file_mode
        self.dir_mode = dir_mode
        self.hash_size = hash_size
        self.key_algo = key_algo
        self.filesystem = filesystem

    def connect(self, pool):
        return HashedDirectoryPool(pool, self)

class HashedDirectoryPool(Pool):
    def __init__(self, pool, hashed_directory):
        self.pool = pool
        self.hd = hashed_directory

    def get_hash_for_key(self, key):
        hash_algo = hashlib.new(self.hd.key_algo)
        hash_algo.update(key)
        return hash_algo.hexdigest()
    
    def get_value_path(self, key):
        hash = self.get_hash_for_key(key)
        return pjoin(self.hd.base_dir, self.pool, hash[:self.hd.hash_size], hash)

    def get_key_path(self, key):
        hash = self.get_hash_for_key(key)
        return pjoin(self.hd.base_dir, self.pool, hash[:self.hd.hash_size], hash + ".key")

    def is_key_basename(self, filename):
        return filename.endswith(".key")


    def get(self, key):
        path = self.get_value_path(key)
        try:
            file = FileProxy(self.hd.filesystem.open(path, "r+b"))
        except IOError as err:
            if err.errno == errno.ENOENT:
                raise KeyError(key)
            else:
                raise
            
        else:
            return FileProxy(file)

    def create(self, key):
        path = self.get_value_path(key)
        directory = os.path.dirname(path)
        fs = self.hd.filesystem
        
        if not fs.isdir(directory):
            try:
                fs.mkdir(directory, self.hd.dir_mode)
            except OSError as err:
                if err.errno != errno.EEXIST:
                    raise


        file = fs.open(path, "wb")
        key_path = self.get_key_path(key)
        with open(key_path, "wb") as keyfile:
            keyfile.write(key)

        return FileProxy(file)

    def delete(self, key):
        path = self.get_value_path(key)
        fs = self.hd.filesystem

        try:
            fs.unlink(path)
        except OSError as err:
            if err.errno == errno.ENOENT:
                raise KeyError(key)

    def iterkeys(self):
        for root, dirs, files in self.hd.filesystem.walk(self.hd.base_dir):
            for name in files:
                if self.is_key_basename(name):
                    full_path = pjoin(root, name)
                    with open(full_path, "rb") as infile:
                        data = infile.read()
                    yield data

    def contains(self, key):
        path = self.get_value_path(key)
        return self.hd.filesystem.isfile(path)


class MemoryBackend(Backend):
    def connect(self, pool):
        return MemoryBackendPool()

class MemoryBackendPool(Pool):
    # get, create, delete, iterkeys, contains
    def __init__(self):
        self.data = {}

    def get(self, key):
        if key not in self.data:
            raise KeyError(key)

        return MemoryProxy(self.data, key)

    def create(self, key):
        self.data[key] = b""
        return MemoryProxy(self.data, key)

    def delete(self, key):
        del self.data[key]

    def iterkeys(self):
        return self.data.iterkeys()

    def contains(self, key):
        return key in self.data


class MemoryProxy(io.BytesIO):
    def __init__(self, base_dict, key):
        self.base_dict = base_dict
        self.key = key
        super(MemoryProxy, self).__init__(base_dict[key])

    def refresh_base_dict(self):
        if self.key in self.base_dict:
            self.base_dict[self.key] = self.getvalue()

    def flush(self):
        super(MemoryProxy, self).flush()
        self.refresh_base_dict()

    def close(self):
        self.flush()
        super(MemoryProxy, self).close()

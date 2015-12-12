
import os


class PosixFilesystem:
    def open(self, *args, **kwargs):
        return open(*args, **kwargs)

    def isdir(self, path):
        return os.path.isdir(path)

    def mkdir(self, path, mode=0o777):
        return os.mkdir(path, mode)

    def isdir(self, path):
        return os.path.isdir(path)

    def isfile(self, path):
        return os.path.isfile(path)

    def unlink(self, path):
        return os.unlink(path)

    def walk(self, path):
        return os.walk(path)

from .exc import Error


class ZKVError(Error):
    pass


class ZKV:
    def __init__(self, backend, key_transforms=None, value_transforms=None):
        self.backend = backend
        if key_transforms is None:
            key_transforms = []

        if value_transforms is None:
            value_transforms = []

        self.key_transforms = key_transforms
        self.value_transforms = value_transforms

    def connect(self, pool):
        return ZKVConnection(self.backend.connect(pool), self.key_transforms, self.value_transforms)

class ZKVConnection:
    def __init__(self, pool, key_transforms, value_transforms):
        self.pool = pool
        self.key_transforms = key_transforms
        self.value_transforms = value_transforms

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __getitem__(self, key):
        for transform in self.key_transforms:
            key = transform.encode(key)

        value = self.pool.get(key)

        for transform in self.value_transforms:
            value = transform.decode(value)

        return value

    def __setitem__(self, key, value):
        for transform in self.key_transforms:
            key = transform.decode(key)

        new = self.pool.create(key)

        for transform in self.value_transforms:
            value = transform.encode(value)

        new.write(value)
        new.close()

    def __delitem__(self, key):
        self.pool.delete(key)

    def __contains__(self, key):
        return self.pool.contains(key)

    def iterkeys(self):
        return self.pool.iterkeys()


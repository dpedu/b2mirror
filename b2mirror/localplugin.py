import os

from b2mirror.base import Provider, Reciever
from b2mirror.common import FileInfo


class LocalProvider(Provider):
    """
    Iterates files on local disk
    """
    max_chunk_size = 8 * 1024 * 1024

    def __init__(self, local_path):
        super(LocalProvider, self).__init__()
        self.local_path = local_path
        self.current_set = (None, [], [])
        self.walker = os.walk(self.local_path)

    def __next__(self):
        if len(self.current_set[2]) > 0:
            file_abs_path = os.path.join(self.current_set[0], self.current_set[2].pop())
            relative_path = file_abs_path[len(self.local_path):]
            return FileInfo(
                file_abs_path,
                relative_path,
                os.path.getsize(file_abs_path),
                int(os.path.getmtime(file_abs_path)),
                # open(file_abs_path, 'rb')
            )
        else:
            self.current_set = self.walker.__next__()
            self.current_set[1].sort(reverse=True)
            self.current_set[2].sort(reverse=True)
            return self.__next__()


class LocalReciever(Reciever):
    def __init__(self):
        raise NotImplemented()

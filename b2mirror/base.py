class Provider(object):
    """
    Base class file queue iterable
    """
    def __init__(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplemented()


class Reciever(object):
    """
    Base class for destinations
    """
    def put_file(self, file_info, purge_historics=False):
        raise NotImplemented()

    def purge_file(self, file_path):
        raise NotImplemented()

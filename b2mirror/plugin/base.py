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

    def teardown(self):
        pass


class Reciever(object):
    """
    Base class for destinations
    """
    def xfer_file(self, f):
        """
        Future-called function that handles a single file. This method should:
        - check if the file is eligible for transfer (not in ignore list etc)
        - return Result.skipped if skipped
        - transfer the file and return Result.ok or Result.failed
        """
        raise NotImplemented()

    def purge(self):
        """
        Delete files on the remote that were not found when scanning the local tree. This will always be called AFTER
        scanning through the local tree and calling xfer_file for every file.
        """
        raise NotImplemented()

    def teardown(self):
        pass

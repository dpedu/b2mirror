import re
from urllib.parse import urlparse
from itertools import islice, filterfalse
from concurrent.futures import ThreadPoolExecutor

from b2mirror.localplugin import LocalProvider
from b2mirror.b2plugin import B2Reciever
from b2mirror.common import results_ok

import logging

"""
How it works:

B2SyncManager manages the transfer

It holds a src and dest object, src objects provide an iterable of FileInfos.

The manager will iterate the set of FileInfos, and pass each to the dest

Dest will upload the file, and inform the manager it was completed

"""


class B2SyncManager(object):

    def __init__(self, source_module, dest_module, exclude_res=None, workers=10):
        """
        :param source_module: subclass instance of b2mirror.base.Provider acting as a file source
        :param dest_module: subclass of b2mirror.base.Receiver acting as a file destination
        :param exclude_res: compiled regular expression objects that file paths will be matched against. Finding a match
                            means skip the file (and delete on the remote).
        :param workers: Number of parallel transfers
        """
        self.log = logging.getLogger("B2SyncManager")
        self.src = source_module
        self.dest = dest_module

        self.exclude_res = [
            re.compile(r'.*\.(DS_Store|pyc|dropbox)$'),
            re.compile(r'.*__pycache__.*'),
            re.compile(r'.*\.dropbox\.cache.*'),
            re.compile(r'.*\.AppleDouble.*')
        ] + (exclude_res if exclude_res else [])

        self.workers = workers

    def sync(self):
        """
        Sync the source to the dest. First uploads new local files, then cleans dead files from the remote.
        """
        # Phase 1 - Upload all local files missing on the remote
        self.sync_up()
        # Phase 2 - Delete files on the remote missing locally
        self.purge_remote()
        # Phase 3 - Tear down the src/dest modules
        self.src.teardown()
        self.dest.teardown()

    def sync_up(self):
        """
        Sync local files to the remote. All files in the DB will be marked as unseen. When a file is found locally it is
        again marked as seen. This state later used to clear deleted files from the destination
        """
        chunk_size = 1000

        # if rel_path matches any of the REs, the filter is True and the file is skipped
        files_source = filterfalse(lambda x: any([pattern.match(x.rel_path) for pattern in self.exclude_res]), self.src)

        while True:
            chunk = list(islice(files_source, chunk_size))

            for item in chunk:
                # long path names can't be put in sqlite
                assert len(item.rel_path) < 1024

            if len(chunk) == 0:
                break

            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                upload_futures = [executor.submit(self.dest.xfer_file, item) for item in chunk]

            for i in upload_futures:
                assert i.result() in results_ok

    def purge_remote(self):
        """
        During upload phase it is expected that destination modules track state of what files have been seen on the
        local end. When local scan + upload is complete, the module uses this state to purge dead files on the remote.
        """
        self.dest.purge()


def sync(source_uri, dest_uri, account_id, app_key, workers=10, exclude=[], compare_method="mtime"):
    source = urlparse(source_uri)
    dest = urlparse(dest_uri)

    source_provider = None
    dest_receiver = None

    if source.scheme == '':  # Plain file URI
        source_provider = LocalProvider(source.path)
    else:
        raise Exception("Sources other than local file paths not supported")

    if dest.scheme == 'b2':  # Plain file URI
        dest_receiver = B2Reciever(bucket=dest.netloc, path=dest.path, account_id=account_id, app_key=app_key,
                                   workers=workers, compare_method=compare_method)
    else:
        raise Exception("Dests other than B2 URIs not yet supported")

    assert source_provider is not None
    assert dest_receiver is not None

    syncer = B2SyncManager(source_provider, dest_receiver,
                           workers=workers, exclude_res=exclude)
    syncer.sync()

import re
import sys
import sqlite3
from urllib.parse import urlparse
from itertools import islice, filterfalse
from concurrent.futures import ThreadPoolExecutor

from b2mirror.localplugin import LocalProvider
from b2mirror.b2plugin import B2Reciever
from b2mirror.common import Result, results_ok

# import logging
# logging.basicConfig(level=logging.INFO)

"""
How it works:

B2SyncManager manages the transfer

It holds a src and dest object, src objects provide an iterable of FileInfos.

The manager will iterate the set of FileInfos, and pass each to the dest

Dest will upload the file, and inform the manager it was completed

"""


class B2SyncManager(object):

    def __init__(self, source_module, dest_module, exclude_res=None, workers=10, compare_method="mtime"):
        """
        :param source_module: subclass instance of b2mirror.base.Provider acting as a file source
        :param dest_module: subclass of b2mirror.base.Receiver acting as a file destination
        :param exclude_res: compiled regular expression objects that file paths will be matched against. Finding a match
                            means skip the file (and delete on the remote).
        :param workers: Number of parallel transfers
        """
        self.src = source_module
        self.dest = dest_module
        self.db = sqlite3.connect('./sync.db', check_same_thread=False)
        self.db.row_factory = B2SyncManager.dict_factory
        self.db.isolation_level = None  # TBD - does it hurt perf?
        self.exclude_res = [
            re.compile(r'.*\.(DS_Store|pyc|dropbox)$'),
            re.compile(r'.*__pycache__.*'),
            re.compile(r'.*\.dropbox\.cache.*'),
            re.compile(r'.*\.AppleDouble.*')
        ] + (exclude_res if exclude_res else [])
        self.workers = workers
        self._init_db()

        self.should_transfer = {
            "mtime": self._should_transfer_mtime,
            "size": self._should_transfer_size
        }[compare_method]

    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def _init_db(self):
        """
        Init the sqlite databsae. Creates missing tables.
        """
        c = self.db.cursor()

        def table_exists(table_name):
            c.execute("SELECT * FROM SQLITE_MASTER WHERE `type`='table' AND `name`=?", (table_name,))
            tables = c.fetchall()
            if len(tables) == 0:
                return False
            return True

        tables = {
            "files": """
            CREATE TABLE `files` (
              `path` varchar(4096) PRIMARY KEY,
              `mtime` INTEGER,
              `size` INTEGER,
              `seen` BOOLEAN
            );"""
        }

        for table_name, table_create_query in tables.items():
            if not table_exists(table_name):
                c.execute(table_create_query)

        c.close()

    def sync(self):
        """
        Sync the source to the dest. First uploads new local files, then cleans dead files from the remote.
        """
        # Phase 1 - Upload all local files missing on the remote
        self.sync_up()
        # Phase 2 - Delete files on the remote missing locally
        self.purge_remote()

    def sync_up(self):
        """
        Sync local files to the remote. All files in the DB will be marked as unseen. When a file is found locally it is
        again marked as seen. This state later used to clear deleted files from the destination
        """
        #print("Syncing from {} to {}".format(self.src, self.dest))

        # Mark all files as unseen
        # Files will be marked as seen as they are processed
        # Later, unseen files will be purged
        c = self.db.cursor()
        c.execute("UPDATE 'files' SET seen=0;")
        c.close()

        chunk_size = 1000

        # if rel_path matches any of the REs, the filter is True and the file is skipped
        files_source = filterfalse(lambda x: any([pattern.match(x.rel_path) for pattern in self.exclude_res]), self.src)

        while True:
            chunk = list(islice(files_source, chunk_size))

            for item in chunk:
                # long path names can't be put in sqlite
                assert len(item.rel_path) < 512

            if len(chunk) == 0:
                break

            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                upload_futures = [executor.submit(self.xfer_file, item) for item in chunk]

            for i in upload_futures:
                assert i.result() in results_ok

    def xfer_file(self, f):
        """
        Future-called function that handles a single file. The file's modification time is checked against the database
        to see if the file has new content that should be uploaded or is untouched since the last sync
        """

        result = Result.failed

        c = self.db.cursor()

        row = c.execute("SELECT * FROM 'files' WHERE `path` = ?;", (f.rel_path,)).fetchone()

        if self.should_transfer(row, f):

            print("Uploading:", f.rel_path)
            try:
                result = self.dest.put_file(f, purge_historics=row is not None)
            except:
                print("Failed:", f.rel_path)
                print("Unexpected error:", sys.exc_info()[0])
                raise

            # The file was uploaded, commit it to the db
            c.execute("REPLACE INTO 'files' VALUES(?, ?, ?, ?);", (f.rel_path, f.mtime, f.size, 1))

        else:
            c.execute("UPDATE 'files' SET seen=1 WHERE `path` = ?;", (f.rel_path,)).fetchone()
            #print("Skipping:", f.rel_path)
            result = Result.skipped

        c.close()

        return result

    def _should_transfer_mtime(self, row, f):
        return not row or row['mtime'] < f.mtime

    def _should_transfer_size(self, row, f):
        return not row or row['size'] != f.size

    def purge_remote(self):
        """
        Delete files on the remote that were not found when scanning the local tree.
        """
        c = self.db.cursor()
        c_del = self.db.cursor()

        for purge_file in c.execute("SELECT * FROM 'files' WHERE seen=0;"):
            print("Delete on remote: ", purge_file["path"])
            self.dest.purge_file(purge_file["path"])
            c_del.execute("DELETE FROM 'files' WHERE path=?;", (purge_file["path"],))

        c_del.close()
        c.close()


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
                                   workers=workers)
    else:
        raise Exception("Dests other than B2 URIs not yet supported")

    assert source_provider is not None
    assert dest_receiver is not None

    syncer = B2SyncManager(source_provider, dest_receiver, workers=workers, exclude_res=exclude, compare_method=compare_method)
    syncer.sync()

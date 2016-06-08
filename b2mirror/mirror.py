import os
import sqlite3
from threading import Thread
from urllib.parse import urlparse
from collections import namedtuple
from b2.api import B2Api
import sys

from concurrent.futures import ThreadPoolExecutor, Future
"""
How it works:

B2SyncManager manages the transfer

It holes a src and dest object, src objects provide an iterable of FileInfos.

The manager will iterate the set of FileInfos, and pass each to the dest

Dest will upload the file, and inform the manager it was completed

"""
class B2SyncManager(object):

    threads = 5

    def __init__(self, source_module, dest_module):
        self.src = source_module
        self.dest = dest_module
        self.db = sqlite3.connect('./sync.db')
        self.db.row_factory = B2SyncManager.dict_factory
        self.db.isolation_level = None # TBD - does it hurt perf?
        self._init_db()
    
    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def _init_db(self):
        c = self.db.cursor()

        def table_exists(table_name):
            c.execute("SELECT * FROM SQLITE_MASTER WHERE `type`='table' AND `name`=?", (table_name,))
            tables = c.fetchall()
            if len(tables)==0:
                return False
            return True

        tables = {
            "files": """
            CREATE TABLE `files` (
              `path` varchar(1024),
              `mtime` INTEGER,
              `size` INTEGER
            );"""
        }

        for table_name, table_create_query in tables.items():
            if not table_exists(table_name):
                c.execute(table_create_query)

        c.close()

    def sync(self):
        print("Syncing from {} to {}".format(self.src, self.dest))
        for f in self.src:
            c = self.db.cursor()
            
            row = c.execute("SELECT * FROM 'files' WHERE `path` = ?;", (f.rel_path,)).fetchone()
            
            if not row or row['mtime'] < f.mtime:

                print("Uploading: ", f.rel_path, end='')
                sys.stdout.flush()

                self.dest.put_file(f)
                f.fp.close()

                print(" ok")

                # The file was uploaded, commit it to the db

                c.execute("INSERT INTO 'files' VALUES(?, ?, ?);", (f.rel_path, f.mtime, f.size,))

            else:
                print("Skipping: ", f.rel_path)

            c.close()


class Provider(object):
    """
    Base class file queue iterable
    """
    def __init__(self):
        pass

FileInfo = namedtuple('FileInfo', ['abs_path', 'rel_path', 'size', 'mtime', 'fp'])

class LocalProvider(Provider):
    """
    Iterates files on local disk
    """
    max_chunk_size = 8*1024*1024
    def __init__(self, local_path):
        super(LocalProvider, self).__init__()
        self.local_path = local_path
        self.current_set = (None, [], [])

    def __iter__(self):
        self.walker = os.walk(self.local_path)
        return self

    def __next__(self):
        if len(self.current_set[2]) > 0:
            file_abs_path = os.path.join(self.current_set[0], self.current_set[2].pop())
            relative_path = file_abs_path[len(self.local_path):]
            return FileInfo(
                file_abs_path,
                relative_path,
                os.path.getsize(file_abs_path),
                int(os.path.getmtime(file_abs_path)),
                open(file_abs_path, 'rb')
            )
        else:
            self.current_set = self.walker.__next__()
            self.current_set[1].sort(reverse=True)
            self.current_set[2].sort(reverse=True)
            return self.__next__()


class B2Provider(Provider):
    """
    Iterates files in bucket
    """

    def __init__(self, accountId, appKey, bucketId, bucketBasePath):
         super(B2Provider, self).__init__()
         raise NotImplemented()


class Reciever(object):
    """
    Base class for destinations
    """

class B2Reciever(Reciever):

    max_chunk_size = 256*1024

    def __init__(self, bucket, path, account_id, app_key):
        super(B2Reciever, self).__init__()
        self.bucket = bucket
        self.path = path
        self.account_id = account_id
        self.app_key = app_key

        self.api = B2Api()
        self.api.authorize_account('production', self.account_id, self.app_key)
        self.bucket = self.api.get_bucket_by_name(self.bucket)

    def put_file(self, file_info):
        #print(">>> {}".format(file_info.abs_path))
        dest_path = os.path.join(self.path, file_info.rel_path).lstrip('/')
        self.bucket.upload_local_file(
                    file_info.abs_path,
                    dest_path
        )

def sync(source_uri, dest_uri, account_id, app_key):
    source = urlparse(source_uri)
    dest = urlparse(dest_uri)
    
    syncer = B2SyncManager(source_uri, dest_uri)

    source_provider = None
    dest_receiver = None

    if source.scheme == '': # Plain file URI
        source_provider = LocalProvider(source.path)
    else:
        raise Exception("Sources other than local file paths not supported")

    if dest.scheme == 'b2': # Plain file URI
        dest_receiver = B2Reciever(bucket=dest.netloc, path=dest.path, account_id=account_id, app_key=app_key)
    else:
        raise Exception("Dests other than B2 URIs not yet supported")



    assert source_provider is not None
    assert dest_receiver is not None
    
    syncer = B2SyncManager(source_provider, dest_receiver)
    syncer.sync()
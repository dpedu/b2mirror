import os
import sys
import logging

from b2.api import B2Api

from b2mirror.base import Provider, Reciever
from b2mirror.common import Result, FileInfo
from b2 import exception as b2exception
from b2.download_dest import DownloadDestLocalFile
import sqlite3
from contextlib import closing


class B2Provider(Provider):
    """
    Iterates files in bucket
    """

    def __init__(self, accountId, appKey, bucketId, bucketBasePath):
        super(B2Provider, self).__init__()
        raise NotImplemented()


class B2Reciever(Reciever):

    max_chunk_size = 256 * 1024

    def __init__(self, bucket, path, account_id, app_key, workers=10, compare_method='mtime'):
        super(B2Reciever, self).__init__()
        self.log = logging.getLogger("B2Reciever")
        self.bucket_name = bucket
        self.path = path.lstrip('/')
        self.account_id = account_id
        self.app_key = app_key

        self.api = B2Api(max_upload_workers=workers)
        self.api.authorize_account('production', self.account_id, self.app_key)
        self.bucket = self.api.get_bucket_by_name(self.bucket_name)

        self.db = None
        self._db_setup()

        # The receiver is responsible to determining if a file needs to be uploaded or not
        self.should_transfer = {
            "mtime": self._should_transfer_mtime,
            "size": self._should_transfer_size
        }[compare_method]

    def _db_setup(self, db_path=None):
        """
        This plugin uses a sqlite database to track the contents of what is on the remote B2 bucket. Why? It's simply
        faster than using B2's quite limited API to perform the same action. The sqlite DB is stored on the bucket.
        This method:
        - Downloads the DB
            - if none present, creates a new db file
        - Initializes/updates tables in the db
        """
        if not db_path:
            db_path = '/tmp/b2mirror.{}.db'.format(os.getpid())
            self.db_path = db_path

        fetch_success = self._fetch_remote_db(db_path)
        self._open_db()

        if not fetch_success:
            # no db was downloaded and the handle above is empty. initialize it.
            self._init_db_contents()
            logging.info("Initialized database")

        # Mark all files as unseen
        # Files will be marked as seen as they are processed
        # Later, unseen files will be purged
        with closing(self.db.cursor()) as c:
            c.execute("UPDATE 'files' SET seen=0;")

    def _open_db(self):
        self.db = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row

    def _init_db_contents(self):
        """
        Init the sqlite database. Creates missing tables.
        """
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

        with closing(self.db.cursor()) as c:
            for table_name, table_create_query in tables.items():
                if not table_exists(table_name):
                    c.execute(table_create_query)

    def _fetch_remote_db(self, db_path):
        db_bucket_path = os.path.join(self.path, ".b2mirror.db")
        self.log.info("Fetching tracking db from bucket ({}) to {}".format(db_bucket_path, db_path))
        try:
            self.bucket.download_file_by_name(db_bucket_path, DownloadDestLocalFile(db_path))
        except b2exception.UnknownError as e:
            if '404 not_found' in e.message:
                return False
            else:
                raise
        return True

    def teardown(self):
        """
        Place the DB file back onto the remote
        """
        self.db.close()
        sqlite_finfo = FileInfo(self.db_path,
                                ".b2mirror.db",
                                os.path.getsize(self.db_path),
                                int(os.path.getmtime(self.db_path)))
        self.put_file(sqlite_finfo, purge_historics=True)
        os.unlink(self.db_path)

    def _should_transfer_mtime(self, row, f):
        return not row or row['mtime'] < f.mtime

    def _should_transfer_size(self, row, f):
        return not row or row['size'] != f.size

    def xfer_file(self, f):
        """
        Future-called function that handles a single file. The file's modification time is checked against the database
        to see if the file has new content that should be uploaded or is untouched since the last sync
        """
        result = Result.failed

        with closing(self.db.cursor()) as c:

            row = c.execute("SELECT * FROM 'files' WHERE `path` = ?;", (f.rel_path,)).fetchone()
            if self.should_transfer(row, f):

                print("Uploading:", f.rel_path)
                try:
                    # upload the file. if a row existed it means there may be historic copies of the file already there
                    result = self.put_file(f, purge_historics=row is not None)
                except:
                    print("Failed:", f.rel_path)
                    print("Unexpected error:", sys.exc_info()[0])
                    raise

                # The file was uploaded, commit it to the db
                c.execute("REPLACE INTO 'files' VALUES(?, ?, ?, ?);", (f.rel_path, f.mtime, f.size, 1))

            else:
                c.execute("UPDATE 'files' SET seen=1 WHERE `path` = ?;", (f.rel_path,)).fetchone()
                result = Result.skipped

            return result

    def put_file(self, file_info, purge_historics=False):
        dest_path = os.path.join(self.path, file_info.rel_path).lstrip('/')
        upload_result = self.bucket.upload_local_file(file_info.abs_path, dest_path)  # NOQA
        if purge_historics:
            self.delete_by_path(dest_path, skip=1)

        return Result.ok

    def purge(self):
        """
        Delete files on the remote that were not found when scanning the local tree. This assumes an upload phase has
        already been doing using ***THIS B2Reciever INSTANCE***.
        """
        with closing(self.db.cursor()) as c:
            with closing(self.db.cursor()) as c_del:

                for purge_file in c.execute("SELECT * FROM 'files' WHERE seen=0;"):
                    print("Delete on remote: ", purge_file["path"])
                    self.purge_file(purge_file["path"])
                    c_del.execute("DELETE FROM 'files' WHERE path=?;", (purge_file["path"],))

    def purge_file(self, file_path):
        """
        Remove a file and all historical copies from the bucket
        :param file_path: File path relative to the source tree to delete. This should NOT include self.path
        """
        dest_path = os.path.join(self.path, file_path).lstrip('/')
        self.delete_by_path(dest_path)

    def delete_by_path(self, file_path, skip=0, max_entries=100):
        """
        List all versions of a file and delete some or all of them
        :param file_path: Bucket path to delete
        :param skip: How many files to skip before starting deletion. 5 means keep 5 historical copies. Using a value
                     of 0 will delete a file and all it's revisions
        :param max_entries:
        """
        for f in self.bucket.list_file_versions(start_filename=file_path, max_entries=max_entries)["files"]:
            if f["fileName"] == file_path:
                if skip == 0:
                    self.api.delete_file_version(f["fileId"], f["fileName"])
                else:
                    skip -= 1
            else:
                return

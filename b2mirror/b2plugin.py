import os

from b2.api import B2Api

from b2mirror.base import Provider, Reciever
from b2mirror.common import Result, results_ok

class B2Provider(Provider):
    """
    Iterates files in bucket
    """

    def __init__(self, accountId, appKey, bucketId, bucketBasePath):
        super(B2Provider, self).__init__()
        raise NotImplemented()


class B2Reciever(Reciever):

    max_chunk_size = 256*1024

    def __init__(self, bucket, path, account_id, app_key, workers=10):
        super(B2Reciever, self).__init__()
        self.bucket_name = bucket
        self.path = path
        self.account_id = account_id
        self.app_key = app_key

        self.api = B2Api(max_upload_workers=workers)
        self.api.authorize_account('production', self.account_id, self.app_key)
        self.bucket = self.api.get_bucket_by_name(self.bucket_name)

    def put_file(self, file_info, purge_historics=False):
        dest_path = os.path.join(self.path, file_info.rel_path).lstrip('/')
        upload_result = self.bucket.upload_local_file(
                    file_info.abs_path,
                    dest_path
        )

        if purge_historics:
            self.delete_by_path(dest_path, skip=1)

        return Result.ok

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
                    skip-=1
            else:
                return

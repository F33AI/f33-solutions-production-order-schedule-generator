# Copyright (C) Fourteen33 Inc. - All Rights Reserved

from io import BytesIO
from typing import Tuple, List, Callable
from google.cloud import storage as gcstorage

class CloudStorageClient:

    def __init__(self):
        self.client = gcstorage.Client()

    def uri(self, path: str) -> str:
        return self._add_gs(path)

    def _add_gs(self, path: str) -> str:
        if not path.startswith("gs://"):
            return f"gs://{path}"
        return path

    def _remove_gs(self, path: str) -> str:
        if path.startswith("gs://"):
            return path[5:]
        return path

    def _get_blob(self, bucket_path: str) -> gcstorage.Blob:
        bucket_name, relative_path = self._split_bucket_path(bucket_path)
        bucket = self.client.get_bucket(bucket_name)
        return bucket.blob(relative_path)

    def _split_bucket_path(self, path: str) -> Tuple[str, str]:
        """ Splits bucket path into bucket name and relative path

            Examples:
            >>> path = "gs://mytestbucket/my/test/path"
            >>> _split_bucket_path(path)
            (mytestbucket, my/test/path)
        """
        if path.startswith("gs://"):
            path = path[5:]
        divider_idx = path.find("/")
        return path[:divider_idx], path[divider_idx + 1:]

    def _pre_upload(self, remote_path: str, overwrite: bool) -> gcstorage.Blob:
        remote_path = self._add_gs(remote_path)
        blob = self._get_blob(remote_path)
        if not overwrite and blob.exists():
            message = "File already exists! Set `overwrite` to True to " \
                      "overwrie the content of a file on GCS.\n\n" \
                      f"Related path: {remote_path}"
            raise FileExistsError(message)
        return blob

    def file_exists(self, remote_path: str) -> bool:
        remote_path = self._add_gs(remote_path)
        blob = self._get_blob(remote_path)
        return blob.exists()

    def upload_file(self, local_path: str, remote_path: str,
                    overwrite: bool = False) -> None:
        blob = self._pre_upload(remote_path, overwrite)
        blob.upload_from_filename(local_path)

    def upload_from_bytes(self, remote_path: str, content_bytes: bytes,
                          overwrite: bool = False) -> None:

        content_bytes = BytesIO(content_bytes)
        content_bytes.seek(0)
        blob = self._pre_upload(remote_path, overwrite)
        blob.upload_from_file(content_bytes)

    def upload_content(self, remote_path: str, content: str,
                       overwrite: bool = False) -> None:
        blob = self._pre_upload(remote_path, overwrite)
        blob.upload_from_string(content)

    def download_file(self, remote_path: str, local_path: str) -> None:
        remote_path = self._add_gs(remote_path)
        blob = self._get_blob(remote_path)
        blob.download_to_filename(local_path)

    def download_content(self, remote_path: str, parse_content: Callable = lambda x: x) -> str:
        remote_path = self._add_gs(remote_path)
        blob = self._get_blob(remote_path)
        return parse_content(blob.download_as_string())

    def list_files(self, remote_path: str) -> List[str]:
        """ Lists all files that are inside a directory on GCP. This is just
            human-friendly workaround since GCP doesn't support files and catalogs.
            All objects are stored as blobs and their 'path' is just a name.

            Note: in some cases it would be probably better to retrieve
                all blob's names at once (instead of calling the API every single time)
                and work on locally cached data. It might be dangerous since we
                would have to update it by hand if there are any changes.
        """
        remote_path = self._add_gs(remote_path)
        bucket_name, remote_relative_path = self._split_bucket_path(remote_path)
        if len(remote_relative_path) and not remote_relative_path.endswith("/"):
            remote_relative_path = f"{remote_relative_path}/"

        bucket = self.client.get_bucket(bucket_name)
        objects = [blob.name[len(remote_relative_path):] for blob in bucket.list_blobs()
                   if blob.name.startswith(remote_relative_path)]
        items = set([object.split("/")[0] for object in objects])
        return items

    def copy_file(self, src_remote_path: str, dst_remote_path: str) -> None:
        src_remote_path = self._add_gs(src_remote_path)
        dst_remote_path = self._add_gs(dst_remote_path)

        src_bucket_name, src_relative_path = self._split_bucket_path(src_remote_path)
        dst_bucket_name, dst_relative_path = self._split_bucket_path(dst_remote_path)
        src_bucket = self.client.get_bucket(src_bucket_name)
        dst_bucket = self.client.get_bucket(dst_bucket_name)
        src_blob = src_bucket.blob(src_relative_path)
        src_bucket.copy_blob(src_blob, dst_bucket, dst_relative_path)

    def with_uri(self, path: str) -> str:
        return self._add_gs(path)

    def without_uri(self, path: str) -> str:
        return self._remove_gs(path)

    def delete_file(self, remote_path: str) -> None:
        remote_path = self._add_gs(remote_path)
        blob = self._get_blob(remote_path)
        blob.delete()
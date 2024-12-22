"""S3 Cloud Storage."""

import os
import traceback
from pathlib import Path
from typing import List, Optional, Union

import boto3

from unicloud.abstract_class import CloudStorageFactory


class S3(CloudStorageFactory):
    """S3 Cloud Storage."""

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: Optional[str] = None,
    ):
        """
        Initialize the AWS S3 client with credentials and region information.

        Parameters
        ----------
        aws_access_key_id: [str]
            Your AWS access key ID.
        aws_secret_access_key: [str]
            Your AWS secret access key.
        region_name: Optional[str]
            The name of the AWS region to connect to. Default is None.
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self._client = self.create_client()

    @property
    def client(self):
        """AWS S3 Client."""
        return self._client

    def create_client(self):
        """Create and returns an AWS S3 client instance.

        initializing the AWS S3 client, passing credentials directly is one option. Another approach is to use AWS
        IAM roles for EC2 instances or to configure the AWS CLI with aws configure, which sets up the credentials'
        file used by boto3. This can be a more secure and manageable way to handle credentials, especially in
        production environments.

        Initialize the S3 client with AWS credentials and region.
        """
        return boto3.client(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def upload(self, local_path: Union[str, Path], bucket_path: str):
        """Upload a file to S3.

        Parameters
        ----------
        local_path: [str]
            The path to the file to upload.
        bucket_path: [str]
            The bucket_path in the format "bucket_name/object_name".
        """
        bucket_name, object_name = bucket_path.split("/", 1)
        try:
            self.client.upload_file(local_path, bucket_name, object_name)
        except Exception as e:
            print("Error uploading file to S3:")
            print(traceback.format_exc())
            raise e
        print(f"File {local_path} uploaded to {bucket_path}.")

    def download(self, bucket_path: str, local_path: Union[str, Path]):
        """Download a file from S3.

        Parameters
        ----------
        bucket_path: [str]
            The bucket_path in the format "bucket_name/object_name".
        local_path: [str]
            The path to save the downloaded file.
        """
        bucket_name, object_name = bucket_path.split("/", 1)
        self.client.download_file(bucket_name, object_name, local_path)
        print(f"File {bucket_path} downloaded to {local_path}.")

    def get_bucket(self, bucket_name: str) -> "Bucket":
        """Retrieve a bucket object."""
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        bucket = s3.Bucket(bucket_name)
        return Bucket(bucket)


class Bucket:
    """
    AWS S3 Bucket interface for file and directory operations.

    This class allows interacting with an S3 bucket for uploading, downloading,
    and deleting files and directories.
    """

    def __init__(self, bucket):  # :boto3.resources("s3").Bucket
        """
        Initialize the Bucket class.

        Parameters
        ----------
        bucket : boto3.resources.factory.s3.Bucket
            A boto3 S3 Bucket resource instance.

        Examples
        --------
        >>> import boto3
        >>> s3 = boto3.resource("s3")
        >>> bucket = Bucket(s3.Bucket("my-bucket"))
        """
        self._bucket = bucket

    @property
    def bucket(self):
        """bucket."""
        return self._bucket

    @property
    def name(self):
        """Bucket name."""
        return self.bucket.name

    def list_files(self, prefix: Optional[str] = None) -> List[str]:
        """List files in the bucket."""
        if prefix is None:
            prefix = ""

        return [obj.key for obj in self.bucket.objects.filter(Prefix=prefix)]

    def upload(
        self, local_path: Union[str, Path], bucket_path: str, overwrite: bool = False
    ):
        """
        Upload a file or directory to the S3 bucket.

        Parameters
        ----------
        local_path : Union[str, Path]
            Path to the local file or directory to upload.
        bucket_path : str
            Path in the bucket to upload to.
        overwrite : bool, optional
            Whether to overwrite existing files. Default is False.

        Raises
        ------
        FileNotFoundError
            If the local path does not exist.
        ValueError
            If attempting to overwrite an existing file and overwrite is False.

        Notes
        -----
        - Uploads a single file or all files within a directory (including subdirectories).

        Examples
        --------
        Upload a single file:
            >>> bucket.upload('local/file.txt', 'bucket/file.txt')  # doctest: +SKIP

        Upload a directory:
            >>> bucket.upload('local/dir', 'bucket/dir')  # doctest: +SKIP
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Path {local_path} does not exist.")

        if local_path.is_file():
            self._upload_file(local_path, bucket_path, overwrite)
        elif local_path.is_dir():
            self._upload_directory(local_path, bucket_path, overwrite)
        else:
            raise ValueError(
                f"Invalid path type: {local_path} is neither a file nor a directory."
            )

    def _upload_file(self, local_path: Path, bucket_path: str, overwrite: bool):
        """Upload a single file."""
        if not overwrite and self.file_exists(bucket_path):
            raise ValueError(f"File {bucket_path} already exists in the bucket.")
        self.bucket.upload_file(Filename=str(local_path), Key=bucket_path)
        print(f"File {local_path} uploaded to {bucket_path}.")

    def _upload_directory(self, local_path: Path, bucket_path: str, overwrite: bool):
        """Upload a directory recursively."""
        for root, _, files in os.walk(local_path):
            for file in files:
                file_path = Path(root) / file
                relative_path = file_path.relative_to(local_path)
                s3_path = f"{bucket_path.rstrip('/')}/{relative_path.as_posix()}"
                self._upload_file(file_path, s3_path, overwrite)

    def download(
        self, bucket_path: str, local_path: Union[str, Path], overwrite: bool = False
    ):
        """
        Download a file or directory from the S3 bucket.

        Parameters
        ----------
        bucket_path : str
            Path in the bucket to download.
        local_path : Union[str, Path]
            Local path to save the downloaded file or directory.
        overwrite : bool, optional
            Whether to overwrite existing local files. Default is False.

        Raises
        ------
        ValueError
            If the local path exists and overwrite is False.

        Notes
        -----
        - If bucket_path is a directory, downloads all files within it recursively.

        Examples
        --------
        Download a single file:
            >>> bucket.download('bucket/file.txt', 'local/file.txt') # doctest: +SKIP

        Download a directory:
            >>> bucket.download('bucket/dir/', 'local/dir/')  # doctest: +SKIP
        """
        local_path = Path(local_path)
        if bucket_path.endswith("/"):
            self._download_directory(bucket_path, local_path, overwrite)
        else:
            self._download_file(bucket_path, local_path, overwrite)

    def _download_file(self, bucket_path: str, local_path: Path, overwrite: bool):
        """Download a single file."""
        if local_path.exists() and not overwrite:
            raise ValueError(f"File {local_path} already exists locally.")

        local_path.parent.mkdir(parents=True, exist_ok=True)

        self.bucket.download_file(Key=bucket_path, Filename=str(local_path))
        print(f"File {bucket_path} downloaded to {local_path}.")

    def _download_directory(self, bucket_path: str, local_path: Path, overwrite: bool):
        """Download a directory recursively."""
        local_path.mkdir(parents=True, exist_ok=True)
        for obj in self.bucket.objects.filter(Prefix=bucket_path):
            if obj.key.endswith("/"):
                continue
            relative_path = Path(obj.key).relative_to(bucket_path)
            self._download_file(obj.key, local_path / relative_path, overwrite)

    def delete(self, bucket_path: str):
        """
        Delete a file or directory from the S3 bucket.

        Parameters
        ----------
        bucket_path : str
            Path in the bucket to delete.

        Raises
        ------
        ValueError
            If the file or directory does not exist.

        Notes
        -----
        - Deletes a single file or all files within a directory.

        Examples
        --------
        Delete a single file:
            >>> bucket.delete('bucket/file.txt')  # doctest: +SKIP

        Delete a directory:
            >>> bucket.delete('bucket/dir/')  # doctest: +SKIP
        """
        if bucket_path.endswith("/"):
            self._delete_directory(bucket_path)
        else:
            self._delete_file(bucket_path)

    def _delete_file(self, bucket_path: str):
        """Delete a single file."""
        obj = self.bucket.Object(bucket_path)
        obj.delete()
        print(f"Deleted {bucket_path}.")

    def _delete_directory(self, bucket_path: str):
        """Delete a directory recursively."""
        for obj in self.bucket.objects.filter(Prefix=bucket_path):
            obj.delete()
            print(f"Deleted {obj.key}.")

    def file_exists(self, bucket_path: str) -> bool:
        """
        Check if a file exists in the bucket.

        Parameters
        ----------
        bucket_path : str
            Path in the bucket to check.

        Returns
        -------
        bool
            True if the file exists, False otherwise.
        """
        objs = list(self.bucket.objects.filter(Prefix=bucket_path))
        return len(objs) > 0 and objs[0].key == bucket_path

"""S3 Cloud Storage."""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from unicloud.abstract_class import AbstractBucket, CloudStorageFactory

logger = logging.getLogger(__name__)


class S3(CloudStorageFactory):
    """S3 Cloud Storage."""

    def __init__(
        self,
        configs: Optional[Dict] = None,
    ):
        """
        Initialize the AWS S3 client with credentials and region information.

        - To initialize the `S3` client, you have to store the credentials in the following
        environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`.

        Raises
        ------
        ValueError
            If required environment variables for AWS credentials or region are not set.

        Notes
        -----
        The client uses environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION`
        for credentials and region configuration by default.

        Examples
        --------
        Create a client with custom configuration:
            >>> from botocore.config import Config
            >>> s3 = S3(client_configs={
            ...     'config': Config(signature_version='s3v4'),
            ...     'region_name': 'us-west-2'
            ... }) # doctest: +SKIP

        References
        ----------
        Set the environment variables required for the AWS CLI:
            https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-envvars.html
        """
        self._client = self.create_client(configs)

    @property
    def client(self):
        """AWS S3 Client."""
        return self._client

    def create_client(self, configs: Optional[Dict] = None) -> boto3.client:
        """Create and returns an AWS S3 client instance.

        initializing the AWS S3 client, passing credentials directly is one option. Another approach is to use AWS
        IAM roles for EC2 instances or to configure the AWS CLI with aws configure, which sets up the credentials'
        file used by boto3. This can be a more secure and manageable way to handle credentials, especially in
        production environments.

        Initialize the S3 client with AWS credentials and region.


        Parameters
        ----------
        configs : dict, optional
            Additional configuration parameters for the boto3 client.
            For example, to set `Config` for unsigned requests:
            `{'config': Config(signature_version=botocore.UNSIGNED)}`


        Returns
        -------
        boto3.client
            An AWS S3 client instance.

        Raises
        ------
        ValueError
            If required environment variables for AWS credentials or region are not set.

        Notes
        -----
        The client uses environment variables `AWS_ACCESS_KEY_ID`,
        `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION` for credentials
        and region configuration by default.

        Examples
        --------
        Create a client with custom configuration:
            >>> from botocore.config import Config
            >>> s3 = S3(client_configs={
            ...     'config': Config(signature_version='s3v4'),
            ...     'region_name': 'us-west-2'
            ... })  # doctest: +SKIP
        """
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        if aws_access_key_id is None:
            raise ValueError("AWS_ACCESS_KEY_ID is not set.")

        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if aws_secret_access_key is None:
            raise ValueError("AWS_SECRET_ACCESS_KEY is not set.")

        region = os.getenv("AWS_DEFAULT_REGION")
        if region is None:
            raise ValueError("AWS_DEFAULT_REGION is not set.")

        # Set defaults and allow overrides through client_configs
        client_params = {
            "service_name": "s3",
            "region_name": region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }
        if configs:
            client_params.update(configs)

        try:
            return boto3.client(**client_params)
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error("AWS credentials not found.")
            raise e

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
            logger.info(f"File {local_path} uploaded to {bucket_path}.")
        except Exception as e:
            logger.error("Error uploading file to S3:", exc_info=True)
            raise e

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
        try:
            self.client.download_file(bucket_name, object_name, local_path)
            logger.info(f"File {bucket_path} downloaded to {local_path}.")
        except Exception as e:
            logger.error("Error downloading file from S3:", exc_info=True)
            raise e

    def get_bucket(self, bucket_name: str) -> "Bucket":
        """Retrieve a bucket object.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket to retrieve.

        Returns
        -------
        Bucket
            A Bucket object for the specified bucket.

        Examples
        --------
        Create the S3 client and get your bucket:
            >>> s3 = S3() # doctest: +SKIP
            >>> bucket = s3.get_bucket("my-bucket") # doctest: +SKIP
        """
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION"),
        )
        bucket = s3.Bucket(bucket_name)
        return Bucket(bucket)


class Bucket(AbstractBucket):
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
        - Initialize the Bucket class with a boto3 S3 Bucket resource instance:

            >>> import boto3
            >>> s3 = boto3.resource("s3")
            >>> bucket = Bucket(s3.Bucket("my-bucket")) # doctest: +SKIP

        - Get the Bucket object from an S3 client:
            >>> s3 = S3() # doctest: +SKIP
            >>> bucket = s3.get_bucket("my-bucket") # doctest: +SKIP
        """
        self._bucket = bucket

    def __str__(self):
        """__str__."""
        return f"Bucket: {self.name}"

    def __repr__(self):
        """__repr__."""
        return f"Bucket: {self.name}"

    @property
    def bucket(self):
        """bucket."""
        return self._bucket

    @property
    def name(self):
        """Bucket name."""
        return self.bucket.name

    def list_files(self, prefix: Optional[str] = None) -> List[str]:
        """
        List files in the bucket with a specific prefix.

        Parameters
        ----------
        prefix : str, optional, default=None
            The prefix to filter files (e.g., 'folder/' to list files under 'folder/').

        Returns
        -------
        List[str]
            A list of file keys matching the prefix.

        Examples
        --------
        Create the S3 client and bucket:
            >>> s3 = S3() # doctest: +SKIP
            >>> bucket = s3.get_bucket("my-bucket") # doctest: +SKIP

        List all files in the bucket:
            >>> bucket.list_files()  # doctest: +SKIP

        List files with a specific prefix:
            >>> bucket.list_files(prefix="folder/")  # doctest: +SKIP
        """
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
            The destination path in the bucket.
        overwrite : bool, optional, default=False
            Whether to overwrite existing files in the bucket.

        Raises
        ------
        FileNotFoundError
            If the local file or directory does not exist.
        ValueError
            If attempting to overwrite an existing file when `overwrite` is False.
            If the local path is a directory and it is empty.

        Notes
        -----
        - Uploads a single file or recursively uploads a directory and its contents.

        Examples
        --------
        Create the S3 client and bucket:
            >>> s3 = S3() # doctest: +SKIP
            >>> bucket = s3.get_bucket("my-bucket") # doctest: +SKIP

        Upload a single file:
            >>> bucket.upload("local/file.txt", "bucket/file.txt", overwrite=False)  # doctest: +SKIP

        Upload a directory:
            >>> bucket.upload("local/dir", "bucket/dir", overwrite=True)  # doctest: +SKIP
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
        logger.info(f"File {local_path} uploaded to {bucket_path}.")

    def _upload_directory(self, local_path: Path, bucket_path: str, overwrite: bool):
        """Upload a directory recursively."""
        if local_path.is_dir() and not any(local_path.iterdir()):
            raise ValueError(f"Directory {local_path} is empty.")

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
            Local destination path for the downloaded file or directory.
        overwrite : bool, optional, default=False
            Whether to overwrite existing local files.

        Raises
        ------
        ValueError
            If the local path exists and `overwrite` is False.
            If the file or directory does not exist in the bucket.

        Notes
        -----
        - Downloads a single file or recursively downloads all files in a directory.

        Examples
        --------
        Create the S3 client and bucket:
            >>> s3 = S3() # doctest: +SKIP
            >>> bucket = s3.get_bucket("my-bucket") # doctest: +SKIP

        Download a single file:
            >>> bucket.download("bucket/file.txt", "local/file.txt", overwrite=False)  # doctest: +SKIP

        Download a directory:
            >>> bucket.download("bucket/dir/", "local/dir/", overwrite=True)  # doctest: +SKIP
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
        logger.info(f"File {bucket_path} downloaded to {local_path}.")

    def _download_directory(self, bucket_path: str, local_path: Path, overwrite: bool):
        """Download a directory recursively."""
        if not any(self.list_files(bucket_path)):
            raise ValueError(f"Directory {bucket_path} is empty.")

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
            The file or directory path in the bucket to delete.
            - If it ends with '/', it is treated as a directory.

        Raises
        ------
        ValueError
             If the file or directory does not exist in the bucket.

        Notes
        -----
        - Deletes a single file or recursively deletes all files in a directory.

        Examples
        --------
        Create the S3 client and bucket:
            >>> s3 = S3() # doctest: +SKIP
            >>> bucket = s3.get_bucket("my-bucket") # doctest: +SKIP

        Delete a single file:
            >>> bucket.delete("bucket/file.txt")  # doctest: +SKIP

        Delete a directory:
            >>> bucket.delete("bucket/dir/")  # doctest: +SKIP
        """
        if bucket_path.endswith("/"):
            self._delete_directory(bucket_path)
        else:
            self._delete_file(bucket_path)

    def _delete_file(self, bucket_path: str):
        """Delete a single file."""
        objects = list(self.bucket.objects.filter(Prefix=bucket_path))
        if not objects or objects[0].key != bucket_path:
            raise ValueError(f"File {bucket_path} not found in the bucket.")
        self.bucket.Object(bucket_path).delete()
        logger.info(f"Deleted: {bucket_path}")

    def _delete_directory(self, bucket_path: str):
        """Delete a directory recursively."""
        objects = list(self.bucket.objects.filter(Prefix=bucket_path))
        if not objects:
            raise ValueError(f"No files found in the directory: {bucket_path}")

        for obj in objects:
            obj.delete()
            print(f"Deleted {obj.key}.")

    def file_exists(self, file_name: str) -> bool:
        """
        Check if a file exists in the bucket.

        Parameters
        ----------
        file_name : str
            The path of the file in the bucket.

        Returns
        -------
        bool
            True if the file exists, False otherwise.

        Examples
        --------
        Create the S3 client and get your bucket:
            >>> s3 = S3() # doctest: +SKIP
            >>> bucket = s3.get_bucket("my-bucket") # doctest: +SKIP

        Check if a file exists in the bucket:
            >>> bucket.file_exists("bucket/file.txt")  # doctest: +SKIP
        """
        objs = list(self.bucket.objects.filter(Prefix=file_name))
        return len(objs) > 0 and objs[0].key == file_name

    def rename(self, old_path: str, new_path: str):
        """
        Rename a file or directory in the S3 bucket.

        This operation renames a file or directory by copying the content to a new path
        and then deleting the original path.

        Parameters
        ----------
        old_path : str
            The current path of the file or directory in the bucket.
        new_path : str
            The new path for the file or directory in the bucket.

        Raises
        ------
        ValueError
            If the source file or directory does not exist.
            If the destination path already exists.

        Notes
        -----
        - For directories, all files and subdirectories are renamed recursively.
        - The operation is atomic for individual files but not for directories.

        Examples
        --------
        Create the S3 client and get your bucket:
            >>> s3 = S3() # doctest: +SKIP
            >>> bucket = s3.get_bucket("my-bucket") # doctest: +SKIP

        Rename a file:
            >>> bucket.rename("bucket/old_file.txt", "bucket/new_file.txt")  # doctest: +SKIP

        Rename a directory:
            >>> bucket.rename("bucket/old_dir/", "bucket/new_dir/")  # doctest: +SKIP
        """
        # Check if the old path exists
        objects = list(self.bucket.objects.filter(Prefix=old_path))
        if not objects:
            raise ValueError(f"The path '{old_path}' does not exist in the bucket.")

        # Check if the new path already exists
        if any(self.bucket.objects.filter(Prefix=new_path)):
            raise ValueError(f"The destination path '{new_path}' already exists.")

        # Perform the rename
        for obj in objects:
            old_object_name = obj.key
            if old_path.endswith("/") and not old_object_name.startswith(old_path):
                continue  # Skip unrelated files
            new_object_name = old_object_name.replace(old_path, new_path, 1)
            # create a copy of the object to the new path
            self.bucket.Object(new_object_name).copy_from(
                CopySource={"Bucket": self.bucket.name, "Key": old_object_name}
            )
            # delete the original object
            obj.delete()

        logger.info(f"Renamed '{old_path}' to '{new_path}'.")

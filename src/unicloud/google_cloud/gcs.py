"""Google Cloud Storage."""

import fnmatch
import os
from pathlib import Path
from typing import List, Optional, Union

from google.cloud import storage
from google.oauth2 import service_account

from unicloud.abstract_class import CloudStorageFactory
from unicloud.google_cloud.secrets_manager import decode


class GCS(CloudStorageFactory):
    """GCS Cloud Storage."""

    def __init__(self, project_id: str, service_key: Optional[str] = None):
        """Initialize the GCS client.

        Parameters
        ----------
        project_id: [str]
            The Google Cloud project name.
        service_key: str, optional, default=None
            The path to your service key file.

        Raises
        ------
        FileNotFoundError
            If the service key file is provided and does not exist.

        Examples
        --------
        To instantiate the `GCS` class with your `project_id` there are three ways to authenticate:
        - You can provide a path to your service key file. The file should be a JSON file with the service account
            credentials. You can provide the path to the file as the `service_key` argument.

            >>> gcs = GCS("my-project-id", service_key="path/to/your/service-account.json") # doctest: +SKIP
            >>> print(gcs) # doctest: +SKIP
            <BlankLine>
                    project_id: my-project-id,
                    Client Scope=(
                        'https://www.googleapis.com/auth/devstorage.full_control',
                        'https://www.googleapis.com/auth/devstorage.read_only',
                        'https://www.googleapis.com/auth/devstorage.read_write'
                        )
                    )
            <BlankLine>

        - If the GOOGLE_APPLICATION_CREDENTIALS is set in your environment variables, you can instantiate the class
        without providing the service key path.

            >>> gcs = GCS("earth-engine-415620") # doctest: +SKIP

        - If you are running your code in a cloud environment, you can set the `SERVICE_KEY_CONTENT` environment variable
        with the content of your service key file encoded using the `unicloud.secret_manager.encode` function.
        """
        self._project_id = project_id
        if service_key is not None and not Path(service_key).exists():
            raise FileNotFoundError(
                f"The service key file {service_key} does not exist"
            )

        self.service_key = service_key
        self._client = self.create_client()

    @property
    def project_id(self) -> str:
        """project_id."""
        return self._project_id

    @property
    def client(self) -> storage.client.Client:
        """client."""
        return self._client

    def __str__(self) -> str:
        """__str__."""
        return f"""
        project_id: {self.project_id},
        Client Scope={self.client.SCOPE})
        """

    def __repr__(self) -> str:
        """__repr__."""
        return f"""
        project_id: {self.project_id},
        Client Scope={self.client.SCOPE})
        """

    @property
    def bucket_list(self) -> List[str]:
        """bucket_list.

            list the buckets

        Returns
        -------
        List[str]
            list of bucket names
        """
        return [bucket.name for bucket in self.client.list_buckets()]

    def create_client(self) -> storage.client.Client:
        """create_client.

            the returned client deals with everything related to the specific project. For Google Cloud Storage,

            authenticating via a service account is the recommended approach. If you're running your code on a Google
            Cloud environment (e.g., Compute Engine, Cloud Run, etc.), the environment's default service account
            might automatically be used, provided it has the necessary permissions. Otherwise, you can set the
            GOOGLE_APPLICATION_CREDENTIALS environment variable to point to your service account JSON key file.

        Returns
        -------
        google cloud storage client object

        Raises
        ------
        ValueError
            If the GOOGLE_APPLICATION_CREDENTIALS and the EE_PRIVATE_KEY and EE_SERVICE_ACCOUNT are not in your env
            variables you have to provide a path to your service account file.
        """
        if self.service_key:
            credentials = service_account.Credentials.from_service_account_file(
                self.service_key
            )
            client = storage.Client(project=self.project_id, credentials=credentials)
        elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            credentials = service_account.Credentials.from_service_account_file(
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            )
            client = storage.Client(project=self.project_id, credentials=credentials)
        elif "SERVICE_KEY_CONTENT" in os.environ:
            # key need to be decoded into a dict/json object
            service_key_content = decode(os.environ["SERVICE_KEY_CONTENT"])
            client = storage.Client.from_service_account_info(service_key_content)
        else:
            raise ValueError(
                "Since the GOOGLE_APPLICATION_CREDENTIALS and the EE_PRIVATE_KEY and EE_SERVICE_ACCOUNT are not in your"
                "env variables you have to provide a path to your service account"
            )

        return client

    def upload(self, local_path: str, bucket_path: str):
        """Upload a file to GCS.

        Parameters
        ----------
        local_path: [str]
            The path to the file to upload.
        bucket_path: [str]
            The path in the bucket, this path has to have the bucket id as the first path of the path.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID)  # doctest: +SKIP
        >>> file_path = "path/to/local/my-file.txt"  # doctest: +SKIP
        >>> bucket_path = f"{Bucket_ID}/my-file.txt"  # doctest: +SKIP
        >>> gcs.upload(file_path, bucket_path) # doctest: +SKIP
        """
        bucket_name, object_name = bucket_path.split("/", 1)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_path)
        print(f"File {local_path} uploaded to {bucket_path}.")

    def download(self, cloud_path, local_path):
        """Download a file from GCS.

        Parameters
        ----------
        cloud_path: [str]
            The source path in the cloud storage.
        local_path: [str]
            The path to save the downloaded file.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
        >>> cloud_path = f"{Bucket_ID}/my-file.txt"
        >>> local_path = "path/to/local/my-file.txt"
        >>> gcs.download(cloud_path, local_path) # doctest: +SKIP
        """
        bucket_name, object_name = cloud_path.split("/", 1)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.download_to_filename(local_path)
        print(f"File {cloud_path} downloaded to {local_path}.")

    def get_bucket(self, bucket_id: str) -> "GCSBucket":
        """get_bucket.

            get_bucket returns the bucket object

        Parameters
        ----------
        bucket_id : [str]
            bucket id

        Returns
        -------
        storage.bucket.Bucket

        Examples
        --------
        >>> my_bucket_id = "datasets"
        >>> gcs = GCS("py-project-id")  # doctest: +SKIP
        >>> bucket_usr = gcs.get_bucket(my_bucket_id)   # doctest: +SKIP
        """
        bucket = storage.Bucket(self.client, bucket_id, user_project=self.project_id)
        return GCSBucket(bucket)


class GCSBucket:
    """GCSBucket."""

    def __init__(self, bucket: storage.bucket.Bucket):
        """Initialize the GCSBucket."""
        self._bucket = bucket

    def __str__(self):
        """__str__."""
        return f"Bucket: {self.bucket.name}"

    def __repr__(self):
        """__repr__."""
        return f"Bucket: {self.bucket.name}"

    @property
    def bucket(self) -> storage.bucket.Bucket:
        """bucket."""
        return self._bucket

    def list_files(self) -> List[str]:
        """list_blobs."""
        return [blob.name for blob in self.bucket.list_blobs()]

    def get_file(self, blob_id) -> storage.blob.Blob:
        """get_blob."""
        return self.bucket.get_blob(blob_id)

    def file_exists(self, file_name: str) -> bool:
        """file_exists.

        Parameters
        ----------
        file_name : [str]
            file name

        Returns
        -------
        bool
            True if the file exists, False otherwise

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID)
        >>> my_bucket = gcs.get_bucket(Bucket_ID)
        >>> print(my_bucket.file_exists("my-file.txt")) # doctest: +SKIP
        False
        """
        blob = self.bucket.get_blob(file_name)
        return False if blob is None else True

    def upload_file(self, local_path: Union[str, Path], bucket_path: Union[str, Path]):
        """Upload a file to GCS.

        Uploads a file or an entire directory to a Google Cloud Storage bucket.

        If the `local_path` is a directory, this method recursively uploads all files
        and subdirectories to the specified `bucket_path` in the GCS bucket.

        Parameters
        ----------
        local_path : Union[str, Path]
            The path to the local file or directory to upload.
            - For a single file, provide the full path to the file (e.g., "path/to/file.txt").
            - For a directory, provide the path to the directory (e.g., "path/to/directory/").
        bucket_path : str
            The destination path in the GCS bucket where the file(s) will be uploaded.
            - For a single file, provide the full path (e.g., "bucket/folder/file.txt").
            - For a directory upload, provide the base path (e.g., "bucket/folder/").

        Raises
        ------
        FileNotFoundError
            If the `local_path` does not exist.
        ValueError
            If the `local_path` is neither a file nor a directory.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID)   # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID)   # doctest: +SKIP

        Upload a single file:
            >>> my_bucket.upload_file("local/file.txt", "bucket/folder/file.txt")  # doctest: +SKIP

        Upload an entire directory:
            >>> my_bucket.upload_file("local/directory/", "bucket/folder/")     # doctest: +SKIP

        Notes
        -----
        - For directory uploads, the relative structure of the local directory will be preserved in the GCS bucket.
        - Ensure the `bucket_path` is valid and writable.

        """
        local_path = Path(local_path)

        if not local_path.exists():
            raise FileNotFoundError(f"The local path {local_path} does not exist.")

        if local_path.is_file():
            # Upload a single file
            blob = self.bucket.blob(bucket_path)
            blob.upload_from_filename(str(local_path))
            print(f"File {local_path} uploaded to {bucket_path}.")
        elif local_path.is_dir():
            # Upload all files in the directory
            for file in local_path.rglob("*"):
                if file.is_file():
                    # Preserve directory structure in the bucket
                    relative_path = file.relative_to(local_path)
                    bucket_file_path = (
                        f"{bucket_path.rstrip('/')}/{relative_path.as_posix()}"
                    )
                    blob = self.bucket.blob(bucket_file_path)
                    blob.upload_from_filename(str(file))
                    print(f"File {file} uploaded to {bucket_file_path}.")
        else:
            raise ValueError(
                f"The local path {local_path} is neither a file nor a directory."
            )

    def download(self, file_name, local_path):
        """Download a file from GCS.

        Downloads a file from a Google Cloud Storage bucket to a local directory or path.

        This method retrieves a file from a specified bucket and saves it to a given local path.
        If the `file_name` points to a directory (ends with a '/'), it recursively downloads all
        files in that directory, preserving the directory structure locally.

        Parameters
        ----------
        file_name : str
            The name of the file or directory to download from the GCS bucket.
            - For a single file, provide its name (e.g., "example.txt").
            - For a directory, provide its name ending with a '/' (e.g., "data/").
        local_path : Union[str, Path]
            The local destination where the file(s) will be saved.
            - For a single file, provide the full path including the file name (e.g., "local/example.txt").
            - For a directory download, provide the base path (e.g., "local/data/").

        Raises
        ------
        FileNotFoundError
            If the specified file or directory does not exist in the bucket.
        ValueError
            If the local path cannot be created or is invalid.

        Examples
        --------
        To download a file or directory from a GCS bucket, you can use the `download` method:
            >>> Bucket_ID = "test-bucket"
            >>> PROJECT_ID = "py-project-id"
            >>> gcs = GCS(PROJECT_ID)  # doctest: +SKIP
            >>> my_bucket = gcs.get_bucket(Bucket_ID)   # doctest: +SKIP

        Download a single file:
            >>> my_bucket.download("example.txt", "local/example.txt")   # doctest: +SKIP

        Download all files in a directory:
            >>> my_bucket.download("data/", "local/data/")   # doctest: +SKIP

        Notes
        -----
        - When downloading a directory, any subdirectories and their files will also be downloaded.
        - The method ensures the creation of required local directories for the downloaded files.
        - This method supports both absolute and relative paths for the local destination.
        - The `file_name` is case-sensitive and must match the exact name in the GCS bucket.

        Warnings
        --------
        Ensure that the provided `local_path` has sufficient disk space for all files
        being downloaded, especially for large directories.

        See Also
        --------
        upload_file : To upload a file from a local path to a GCS bucket.

        """
        if file_name.endswith("/"):
            blobs = self.bucket.list_blobs(prefix=file_name)
            for blob in blobs:
                if blob.name.endswith("/"):
                    continue

                # Remove the directory prefix to get the relative path
                relative_path = Path(blob.name).relative_to(file_name)
                local_file_path = local_path / relative_path

                # Ensure the directory structure exists
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(local_file_path)
                print(f"Downloaded {blob.name} to {local_file_path}.")
        else:
            # Single file download
            blob = self.bucket.blob(file_name)
            blob.download_to_filename(local_path)
            print(f"File {file_name} downloaded to {local_path}.")

    def delete_file(self, file_path: str):
        """
        Delete a file or all files in a directory from the GCS bucket.

        If the `file_path` ends with a '/', it is treated as a directory, and all files
        within that directory (including subdirectories) are deleted. Otherwise, it deletes
        the specified file.

        Parameters
        ----------
        file_path : str
            The path to the file or directory in the GCS bucket.
            - For a single file, provide the file name (e.g., "example.txt").
            - For a directory, provide the path ending with '/' (e.g., "data/").

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID) # doctest: +SKIP
        >>> my_bucket.delete_file("my-file.txt") # doctest: +SKIP

        Delete a single file:
            >>> my_bucket.delete_file("example.txt") # doctest: +SKIP

        Delete a directory and its contents:
            >>> my_bucket.delete_file("data/") # doctest: +SKIP

        Raises
        ------
        ValueError
            If the specified path is invalid or not found in the bucket.
        """
        if file_path.endswith("/"):
            # Delete all files in the directory
            blobs = self.bucket.list_blobs(prefix=file_path)
            deleted_files = []
            for blob in blobs:
                blob.delete()
                deleted_files.append(blob.name)
                print(f"Deleted file: {blob.name}")

            if not deleted_files:
                raise ValueError(f"No files found in the directory: {file_path}")
        else:
            # Delete a single file
            blob = self.bucket.blob(file_path)
            if blob.exists():
                blob.delete()
                print(f"Blob {file_path} deleted.")
            else:
                raise ValueError(f"File {file_path} not found in the bucket.")

    def search(self, pattern: str = "*", directory: Optional[str] = None) -> List[str]:
        """Find files in the bucket matching a pattern, optionally within a specific directory.

        Parameters
        ----------
        pattern : str
            The pattern to match files against (e.g., '*.txt', 'data/*.json').
        directory : Optional[str]
            The directory in the bucket to search within (e.g., 'data/', 'logs/').
            If None, the entire bucket is searched.

        Returns
        -------
        List[str]
            List of file names in the bucket that match the pattern.

        Examples
        --------
        - Initialize the GCS object

            >>> bucket = "my-bucket"
            >>> PROJECT_ID = "my-project-id"
            >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
            >>> my_bucket = gcs.get_bucket(bucket) # doctest: +SKIP

        - Glob across the entire bucket

            >>> matching_files = my_bucket.search("*.txt") # doctest: +SKIP

        - Glob within a specific directory

            >>> matching_files = my_bucket.search("*.txt", directory="data/")   # doctest: +SKIP
        """
        # Ensure the directory ends with a slash if specified
        if directory and not directory.endswith("/"):
            directory += "/"

        # Use prefix to narrow down results if a directory is specified
        prefix = directory if directory else ""
        blobs = self.bucket.list_blobs(prefix=prefix)

        # Filter blobs by the pattern
        return [
            blob.name
            for blob in blobs
            if fnmatch.fnmatch(blob.name, f"{prefix}{pattern}")
        ]

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

    def __init__(self, project_id: str, service_key_path: Optional[str] = None):
        """Initialize the GCS client.

        Parameters
        ----------
        project_id: [str]
            The Google Cloud project name.
        service_key_path: str, optional, default=None
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

            >>> gcs = GCS("my-project-id", service_key_path="path/to/your/service-account.json") # doctest: +SKIP
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
        if service_key_path is not None and not Path(service_key_path).exists():
            raise FileNotFoundError(
                f"The service key file {service_key_path} does not exist"
            )

        self.service_key = service_key_path
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
        """
        Return a string representation of the GCS client.

        Returns
        -------
        str
            A string with the project ID and client scope.

        Examples
        --------
        >>> PROJECT_ID = "my-project-id"
        >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
        >>> print(str(gcs))  # doctest: +SKIP
        project_id: my-project-id,
        Client Scope=(...)
        """
        return f"""
        project_id: {self.project_id},
        Client Scope={self.client.SCOPE})
        """

    def __repr__(self) -> str:
        """__repr__.

        Return a detailed string representation of the GCS client.

        Returns
        -------
        str
            A string representation of the GCS client.

        Examples
        --------
        >>> PROJECT_ID = "my-project-id"
        >>> gcs = GCS(PROJECT_ID)
        >>> print(repr(gcs))  # doctest: +SKIP
        project_id: my-project-id,
        Client Scope=(...)
        """
        return f"""
        project_id: {self.project_id},
        Client Scope={self.client.SCOPE})
        """

    @property
    def bucket_list(self) -> List[str]:
        """bucket_list.

         List all bucket names in the project.

        Returns
        -------
        List[str]
            A list of bucket names accessible in the project.

        Examples
        --------
        >>> PROJECT_ID = "my-project-id"
        >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
        >>> print(gcs.bucket_list)  # doctest: +SKIP
        ['bucket1', 'bucket2', 'bucket3']
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

    def get_bucket(self, bucket_id: str) -> "Bucket":
        """get_bucket.

        Retrieve a bucket object by its ID.

        Parameters
        ----------
        bucket_id : str
            The ID of the bucket to retrieve.

        Returns
        -------
        Bucket
            A `Bucket` object representing the specified bucket.

        Examples
        --------
        >>> PROJECT_ID = "my-project-id"
        >>> BUCKET_ID = "my-bucket-id"
        >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
        >>> bucket = gcs.get_bucket(BUCKET_ID)  # doctest: +SKIP
        >>> print(bucket)  # doctest: +SKIP
        Bucket: my-bucket-id
        """
        bucket = storage.Bucket(self.client, bucket_id, user_project=self.project_id)
        return Bucket(bucket)


class Bucket:
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

    def list_files(
        self,
        prefix: Optional[str] = None,
        max_results: Optional[int] = None,
        pattern: Optional[str] = None,
    ) -> List[str]:
        """
        List files in the GCS bucket with optional filtering and limits.

        Parameters
        ----------
        prefix : Optional[str]
            A prefix to filter files (e.g., 'folder/' to list files under 'folder/').
        max_results : Optional[int]
            Maximum number of files to list.
        pattern : Optional[str]
            A glob pattern to filter files (e.g., '*.txt', 'data/*.csv').

        Returns
        -------
        List[str]
            A list of file names in the bucket.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID)
        >>> my_bucket = gcs.get_bucket(Bucket_ID)

        List all files in the bucket:
            >>> files = my_bucket.list_files()  # doctest: +SKIP

        List files in a specific folder:
            >>> files = my_bucket.list_files(prefix="data/")    # doctest: +SKIP

        List the first 10 files:
            >>> files = my_bucket.list_files(max_results=10)    # doctest: +SKIP
        """
        blobs = self.bucket.list_blobs(prefix=prefix, max_results=max_results)
        file_names = [blob.name for blob in blobs]

        # Apply pattern matching if a pattern is provided
        if pattern:
            file_names = [name for name in file_names if fnmatch.fnmatch(name, pattern)]

        return file_names

    def get_file(self, blob_id) -> storage.blob.Blob:
        """
        Retrieve a blob object from the bucket.

        Parameters
        ----------
        blob_id : str
            The identifier of the file (blob) in the bucket.

        Returns
        -------
        storage.blob.Blob
            The blob object corresponding to the given `blob_id`.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID)
        >>> my_bucket = gcs.get_bucket(Bucket_ID)
        >>> blob = my_bucket.get_file("example.txt")  # doctest: +SKIP
        >>> print(blob.name)  # doctest: +SKIP
        "example.txt"
        """
        return self.bucket.get_blob(blob_id)

    def file_exists(self, file_name: str) -> bool:
        """file_exists.

        Check if a file exists in the bucket.

        Parameters
        ----------
        file_name : str
            The name of the file to check.

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
        >>> my_bucket.file_exists("example.txt")  # doctest: +SKIP
        True
        >>> my_bucket.file_exists("nonexistent.txt")  # doctest: +SKIP
        False
        """
        blob = self.bucket.get_blob(file_name)
        return False if blob is None else True

    def upload(
        self,
        local_path: Union[str, Path],
        bucket_path: Union[str, Path],
        overwrite: bool = False,
    ):
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
        overwrite : bool, optional
            If True, overwrite existing files. Default is False.

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
            >>> my_bucket.upload("local/file.txt", "bucket/folder/file.txt")  # doctest: +SKIP

        Upload an entire directory:
            >>> my_bucket.upload("local/directory/", "bucket/folder/")     # doctest: +SKIP

        Notes
        -----
        - For directory uploads, the relative structure of the local directory will be preserved in the GCS bucket.
        - Ensure the `bucket_path` is valid and writable.
        """
        local_path = Path(local_path)

        if not local_path.exists():
            raise FileNotFoundError(f"The local path {local_path} does not exist.")

        if local_path.is_file():
            self._upload_file(local_path, bucket_path, overwrite)
        elif local_path.is_dir():
            self._upload_directory(local_path, bucket_path, overwrite)
        else:
            raise ValueError(
                f"The local path {local_path} is neither a file nor a directory."
            )

    def _upload_file(
        self, local_path: Path, bucket_path: str, overwrite: bool = False
    ) -> None:
        """
        Upload a single file to GCS with overwrite handling.

        Parameters
        ----------
        local_path : Path
            The local file to upload.
        bucket_path : str
            The destination path in the GCS bucket.
        overwrite : bool
            If True, overwrites the file if it already exists in the bucket. If False, raises
            a `ValueError` if the destination file already exists.

        Raises
        ------
        ValueError
            If the file exists in the bucket and `overwrite` is False.
        FileNotFoundError
            If the `local_path` does not exist or is not a file.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID) # doctest: +SKIP
        Upload a single file, allowing overwrites:
            >>> my_bucket._upload_file(
            ...     Path("local/file.txt"),
            ...     "bucket/folder/file.txt",
            ...     overwrite=True
            ... ) # doctest: +SKIP

        Upload a single file without overwrites:
            >>> try:
            ...     my_bucket._upload_file(
            ...         Path("local/file.txt"),
            ...         "bucket/folder/file.txt",
            ...         overwrite=False
            ...     ) # doctest: +SKIP
            ... except ValueError as e:
            ...     print(e)
            "The file 'bucket/folder/file.txt' already exists in the bucket and overwrite is set to False."
        """
        blob = self.bucket.blob(bucket_path)

        if not overwrite and blob.exists():
            raise ValueError(
                f"The file '{bucket_path}' already exists in the bucket and overwrite is set to False."
            )

        blob.upload_from_filename(str(local_path))
        print(f"File '{local_path}' uploaded to '{bucket_path}'.")

    def _upload_directory(
        self, local_path: Path, bucket_path: str, overwrite: bool = False
    ):
        """
        Upload an entire directory, including subdirectories, to GCS.

        Parameters
        ----------
        local_path : Path
            The path to the local directory to upload.
        bucket_path : str
            The base destination path in the GCS bucket where the directory contents will be uploaded.
            Directory structure is preserved relative to `local_path`.
        overwrite : bool
            If True, overwrites existing files in the bucket. If False, raises a `ValueError`
            for any existing files.

        Raises
        ------
        FileNotFoundError
            If the `local_path` does not exist or is not a directory.
        ValueError
            If any destination file exists in the bucket and `overwrite` is False.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID) # doctest: +SKIP
        Upload a directory with overwrites allowed:
            >>> my_bucket._upload_directory(
            ...     Path("local/directory/"),
            ...     "bucket/folder/",
            ...     overwrite=True
            ... ) # doctest: +SKIP

        Upload a directory without overwrites:
            >>> try:
            ...     my_bucket._upload_directory(
            ...         Path("local/directory/"),
            ...         "bucket/folder/",
            ...         overwrite=False
            ...     ) # doctest: +SKIP
            ... except ValueError as e:
            ...     print(e)
            "The file 'bucket/folder/subdir/file.txt' already exists in the bucket and overwrite is set to False."
        """
        for file in local_path.rglob("*"):
            if file.is_file():
                relative_path = file.relative_to(local_path)
                bucket_file_path = (
                    f"{bucket_path.rstrip('/')}/{relative_path.as_posix()}"
                )
                self._upload_file(file, bucket_file_path, overwrite)

    def download(self, file_name: str, local_path: str, overwrite: bool = False):
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
        overwrite : bool, optional, default is False.
            If True, overwrites existing local files. Default is False.

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
        upload : To upload a file from a local path to a GCS bucket.

        """
        if file_name.endswith("/"):
            self._download_directory(file_name, local_path, overwrite)
        else:
            self._download_file(file_name, local_path, overwrite)

    def _download_file(
        self, bucket_path: str, local_path: Union[str, Path], overwrite: bool = False
    ) -> None:
        """
        Download a single file from GCS.

        Parameters
        ----------
        bucket_path : str
            The source file in the GCS bucket.
        local_path : Union[str, Path]
            The local destination for the downloaded file.
        overwrite : bool, optional, default is False.
            If True, overwrites the file if it already exists.

        Raises
        ------
        FileNotFoundError
            If the source file does not exist in the bucket.
        ValueError
            If the destination path exists and overwrite is False.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID)  # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID)   # doctest: +SKIP
        >>> my_bucket._download_file(
        ...     "example.txt",
        ...     Path("local/example.txt"),
        ...     overwrite=True
        ... )  # doctest: +SKIP
        """
        local_path = Path(local_path)
        blob = self.bucket.blob(bucket_path)

        if not blob.exists():
            raise FileNotFoundError(
                f"The file '{bucket_path}' does not exist in the bucket."
            )

        if local_path.exists() and not overwrite:
            raise ValueError(
                f"The destination file '{local_path}' already exists and overwrite is set to False."
            )

        local_path.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local_path))
        print(f"File '{bucket_path}' downloaded to '{local_path}'.")

    def _download_directory(
        self, cloud_path: str, local_path: Union[str, Path], overwrite: bool = False
    ) -> None:
        """
        Download a directory from GCS.

        Parameters
        ----------
        cloud_path : str
            The source directory in the GCS bucket.
        local_path : Union[str, Path]
            The local destination for the downloaded directory.
        overwrite : bool, optional, default is False.
            If True, overwrites existing local files.

        Raises
        ------
        FileNotFoundError
            If the source directory does not exist in the bucket.
        ValueError
            If the destination path exists and overwrite is False.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID)  # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID)   # doctest: +SKIP
        >>> my_bucket._download_directory(
        ...     "data/",
        ...     Path("local/data/"),
        ...     overwrite=False
        ... ) # doctest: +SKIP
        """
        local_path = Path(local_path)
        blobs = list(self.bucket.list_blobs(prefix=cloud_path))

        if not any(blobs):
            raise FileNotFoundError(
                f"The directory '{cloud_path}' does not exist in the bucket."
            )

        for blob in blobs:
            if blob.name.endswith("/"):
                continue  # Skip "directory" entries

            relative_path = Path(blob.name).relative_to(cloud_path)
            local_file_path = local_path / relative_path

            if local_file_path.exists() and not overwrite:
                raise ValueError(
                    f"The destination file '{local_file_path}' already exists and overwrite is set to False."
                )

            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(local_file_path)
            print(f"File '{blob.name}' downloaded to '{local_file_path}'.")

    def delete(self, file_path: str):
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

        Raises
        ------
        ValueError
            If the file or directory does not exist in the bucket.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID) # doctest: +SKIP
        >>> my_bucket.delete("my-file.txt") # doctest: +SKIP

        Delete a single file:
            >>> my_bucket.delete("example.txt") # doctest: +SKIP

        Delete a directory and its contents:
            >>> my_bucket.delete("data/") # doctest: +SKIP

         Notes
        -----
        - For directories, all files and subdirectories are deleted recursively.
        - Deleting a non-existent file or directory raises a `ValueError`.
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

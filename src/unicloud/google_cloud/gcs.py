"""Google Cloud Storage."""

import os
from pathlib import Path
from typing import List, Optional

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

    def upload_file(self, local_path, bucket_path):
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
        >>> gcs = GCS(PROJECT_ID)   # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID)   # doctest: +SKIP
        >>> local_path = "path/to/local/my-file.txt"
        >>> bucket_path = "my-file.txt"
        >>> my_bucket.upload_file(file_path, bucket_path) # doctest: +SKIP
        """
        blob = self.bucket.blob(bucket_path)
        blob.upload_from_filename(local_path)
        print(f"File {local_path} uploaded to {bucket_path}.")

    def download_file(self, file_name, local_path):
        """Download a file from GCS.

        Parameters
        ----------
        file_name: str
            The name of the file to download.
        local_path: str
            The path to save the downloaded file.

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID)  # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID)   # doctest: +SKIP
        >>> file_name = "my-file.txt"
        >>> local_path = "path/to/local/my-file.txt"
        >>> my_bucket.download_file(file_name, local_path)  # doctest: +SKIP
        """
        blob = self.bucket.blob(file_name)
        blob.download_to_filename(local_path)
        print(f"File {file_name} downloaded to {local_path}.")

    def delete_file(self, blob_id: str):
        """delete_blob.

        Parameters
        ----------
        blob_id : [str]
            blob id

        Examples
        --------
        >>> Bucket_ID = "test-bucket"
        >>> PROJECT_ID = "py-project-id"
        >>> gcs = GCS(PROJECT_ID) # doctest: +SKIP
        >>> my_bucket = gcs.get_bucket(Bucket_ID) # doctest: +SKIP
        >>> my_bucket.delete_file("my-file.txt") # doctest: +SKIP
        """
        blob = self.bucket.blob(blob_id)
        blob.delete()
        print(f"Blob {blob_id} deleted.")

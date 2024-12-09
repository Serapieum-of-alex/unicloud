"""Google Cloud Storage."""

import os
from pathlib import Path
from typing import Optional

from google.cloud import storage
from google.oauth2 import service_account

from unicloud.abstract_class import CloudStorageFactory
from unicloud.secret_manager import decode


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
        if service_key is not None:
            if not Path(service_key).exists():
                raise FileNotFoundError(
                    f"The service key file {service_key} does not exist"
                )

        self.service_key = service_key
        self._client = self.create_client()

    @property
    def project_id(self):
        """project_id."""
        return self._project_id

    @property
    def client(self):
        """client."""
        return self._client

    def __str__(self):
        """__str__."""
        return f"""
        project_id: {self.project_id},
        Client Scope={self.client.SCOPE})
        """

    def __repr__(self):
        """__repr__."""
        return f"""
        project_id: {self.project_id},
        Client Scope={self.client.SCOPE})
        """

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
            # client = storage.Client(project=self.project_name)
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

    def upload(self, file_path: str, destination: str):
        """Upload a file to GCS.

        Parameters
        ----------
        file_path: [str]
            The path to the file to upload.
        destination: [str]
            The destination path in the cloud storage.
        """
        bucket_name, object_name = destination.split("/", 1)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(file_path)
        print(f"File {file_path} uploaded to {destination}.")

    def download(self, source, file_path):
        """Download a file from GCS.

        Parameters
        ----------
        source: [str]
            The source path in the cloud storage.
        file_path: [str]
            The path to save the downloaded file.
        """
        bucket_name, object_name = source.split("/", 1)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.download_to_filename(file_path)
        print(f"File {source} downloaded to {file_path}.")

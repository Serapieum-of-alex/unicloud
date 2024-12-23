"""This module contains the abstract class for cloud storage factory."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union


class CloudStorageFactory(ABC):
    """Abstract class for cloud storage factory."""

    @abstractmethod
    def create_client(self):
        """Create the cloud storage client."""
        pass

    @abstractmethod
    def upload(self, file_path, destination):
        """Upload data to the cloud storage.

        Parameters:
        - file_path: The path to the file to upload.
        - destination: The destination path in the cloud storage.
        """
        pass

    @abstractmethod
    def download(self, source, file_path):
        """Download data from the cloud storage.

        Parameters:
        - source: The source path in the cloud storage.
        - file_path: The path to save the downloaded file.
        """
        pass


class AbstractBucket(ABC):
    """Abstract class for cloud storage bucket."""

    @abstractmethod
    def __str__(self):
        """Return the name of the bucket."""
        pass

    @abstractmethod
    def __repr__(self):
        """Return the name of the bucket."""
        pass

    @abstractmethod
    def upload(
        self,
        local_path: Union[str, Path],
        bucket_path: Union[str, Path],
        overwrite: bool = False,
    ):
        """Upload a file/directory to the bucket."""
        pass

    @abstractmethod
    def download(
        self, bucket_path: str, local_path: Union[str, Path], overwrite: bool = False
    ):
        """Download a file/directory from the bucket."""
        pass

    @abstractmethod
    def delete(self, bucket_path: str):
        """Delete a file/directory from the bucket."""
        pass

    @abstractmethod
    def list_files(self):
        """List the files/directory in the bucket."""
        pass

    @abstractmethod
    def file_exists(self):
        """Check if a file/directory exists in the bucket."""
        pass

    @property
    @abstractmethod
    def name(self):
        """Get the name of the bucket."""
        pass

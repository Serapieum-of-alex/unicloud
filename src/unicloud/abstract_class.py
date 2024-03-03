""" This module contains the abstract class for cloud storage factory. """
from abc import ABC, abstractmethod


class CloudStorageFactory(ABC):
    """Abstract class for cloud storage factory."""

    @abstractmethod
    def create_client(self):
        """
        Create the cloud storage client.
        """
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

""" Tests for the GCS class. """

import os
import uuid
from unittest.mock import MagicMock, Mock, patch

import pytest

from unicloud.google_cloud.gcs import GCS

MY_TEST_BUCKET = "testing-repositories"
PROJECT_ID = "earth-engine-415620"


class TestGCSMock:
    @pytest.fixture(autouse=True)
    def mock_gcs_client(self):
        """Mock the create_client method for all tests."""
        with patch("unicloud.google_cloud.gcs.GCS.create_client") as mock_create:
            mock_create.return_value = Mock()
            yield mock_create

    def test_gcs_init_without_key(self, mock_gcs_client):
        """Test GCS initialization without a service key."""
        project_id = "test-project"
        gcs = GCS(project_id)
        mock_gcs_client.assert_called_once()
        assert gcs.project_id == project_id
        assert gcs.service_key is None

    @patch("pathlib.Path.exists")
    def test_gcs_init_with_key(self, mock_exists, mock_gcs_client):
        """Test GCS initialization with a service key."""

        # Mock os.path.exists to return True, as if the service_key file exists
        mock_exists.return_value = True
        project_id = "test-project"
        service_key = "/fake/path/to/service_account.json"
        gcs = GCS(project_id, service_key)

        mock_gcs_client.assert_called_once()
        assert gcs.project_id == project_id
        assert gcs.service_key == service_key

    @patch("pathlib.Path.exists")
    @patch("google.cloud.storage.blob.Blob.upload_from_filename")
    @patch("unicloud.google_cloud.gcs.GCS.client")
    def test_upload(self, mock_client, mock_upload, mock_exists):
        """Test the upload_data method."""
        mock_exists.return_value = True
        project_name = "test-project"
        service_key = "/fake/path/to/service_account.json"
        gcs = GCS(project_name, service_key)

        # Configure mocks
        bucket_mock = Mock()
        mock_client.bucket.return_value = bucket_mock
        blob_mock = Mock()
        bucket_mock.blob.return_value = blob_mock
        blob_mock.upload_from_filename = mock_upload

        # Test data
        file_path = "path/to/local/file.txt"
        destination = "test-bucket/test-object"

        # Call the method
        gcs.upload(file_path, destination)

        # Assens
        mock_client.bucket.assert_called_with("test-bucket")
        bucket_mock.blob.assert_called_with("test-object")
        mock_upload.assert_called_with(file_path)

    @patch("pathlib.Path.exists")
    @patch("google.cloud.storage.blob.Blob.download_to_filename")
    @patch("unicloud.google_cloud.gcs.GCS.client")
    def test_download(self, mock_client, mock_download, mock_exists):
        """Test the download_data method."""
        mock_exists.return_value = True
        project_name = "test-project"
        service_key = "/fake/path/to/service_account.json"
        gcs = GCS(project_name, service_key)

        # Configure mocks
        bucket_mock = Mock()
        mock_client.bucket.return_value = bucket_mock
        blob_mock = Mock()
        bucket_mock.blob.return_value = blob_mock
        blob_mock.download_to_filename = mock_download

        # Test data
        source = "test-bucket/test-object"
        file_path = "path/to/downloaded/file.txt"

        # Call the method
        gcs.download(source, file_path)

        # Assertions
        mock_client.bucket.assert_called_with("test-bucket")
        bucket_mock.blob.assert_called_with("test-object")
        mock_download.assert_called_with(file_path)

    def test__str__(self, mock_gcs_client):
        """Test the __str__ method."""
        project_id = "test-project"
        gcs = GCS(project_id)
        assert isinstance(gcs.__str__(), str)
        assert isinstance(gcs.__repr__(), str)

    @patch("unicloud.google_cloud.gcs.GCS.client")
    def test_bucket_list(self, gcs_client):
        """
        The test mocks the GCS.client local property as gcs_client (the GCS.client covers the origial storage.client.Client
        object from google.cloud.storage.client.Client)
        Inside the test the list_buckets method from the original Client (self.client.list_buckets()) is mocked to
        return a list of mocked buckets
        `return [bucket.name for bucket in self.client.list_buckets()]`
        `return [bucket.name for bucket in [mock_bucket1, mock_bucket2, mock_bucket3]]`
        """
        # Create a mock bucket
        mock_bucket1 = MagicMock()
        mock_bucket1.name = "bucket-1"
        mock_bucket2 = MagicMock()
        mock_bucket2.name = "bucket-2"
        mock_bucket3 = MagicMock()
        mock_bucket3.name = "bucket-3"

        # Mock the list_buckets method
        gcs_client.list_buckets.return_value = [
            mock_bucket1,
            mock_bucket2,
            mock_bucket3,
        ]

        gcs = GCS("test-project")
        gcs_client._client = gcs_client

        assert gcs.bucket_list == ["bucket-1", "bucket-2", "bucket-3"]
        gcs_client.list_buckets.assert_called_once()


class TestGCSE2E:
    project_id = PROJECT_ID
    bucket_name = MY_TEST_BUCKET
    # Generate a unique filename to avoid conflicts
    test_file_name = f"test-file-{uuid.uuid4()}.txt"
    test_file_content = "This is a test file."

    @classmethod
    def setup_class(cls):
        """Set up resources before running tests."""
        # Assuming your GCS class initialization looks something like this:
        cls.gcs = GCS(cls.project_id)

        # Create a local file to upload
        with open(cls.test_file_name, "w") as f:
            f.write(cls.test_file_content)

    @classmethod
    def teardown_class(cls):
        """Clean up resources after tests."""
        # Delete the test file from the bucket
        # cls.gcs.delete_data(f"{cls.bucket_name}/{cls.test_file_name}")
        # Remove the local test file
        os.remove(cls.test_file_name)

    def test_upload_and_download(self):
        """Test uploading and downloading a file to/from GCS."""
        # Upload the file
        self.gcs.upload(
            self.test_file_name, f"{self.bucket_name}/{self.test_file_name}"
        )

        # Download the file to a new location
        download_path = f"downloaded-{self.test_file_name}"
        self.gcs.download(f"{self.bucket_name}/{self.test_file_name}", download_path)

        # Verify the content of the downloaded file
        with open(download_path, "r") as f:
            downloaded_content = f.read()

        assert downloaded_content == self.test_file_content

        # Clean up the downloaded file
        os.remove(download_path)

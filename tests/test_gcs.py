""" Tests for the GCS class. """
import os
import uuid
import pytest
from unittest.mock import Mock, patch
from unicloud.gcs import GCS

MY_TEST_BUCKET = "testing-repositories"
PROJECT_ID = "earth-engine-415620"


class TestGCS:
    @pytest.fixture(autouse=True)
    def mock_gcs_client(self):
        """Mock the create_client method for all tests."""
        with patch("unicloud.gcs.GCS.create_client") as mock_create:
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
    @patch("unicloud.gcs.GCS.client")
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
    @patch("unicloud.gcs.GCS.client")
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

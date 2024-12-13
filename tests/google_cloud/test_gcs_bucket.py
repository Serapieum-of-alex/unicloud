import os
import shutil
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from google.cloud import storage

from unicloud.google_cloud.gcs import GCS, GCSBucket

MY_TEST_BUCKET = "testing-repositories"
PROJECT_ID = "earth-engine-415620"


class TestGCSBucketE2E:

    @classmethod
    def setup_class(cls):
        """Set up resources before running tests."""
        client = GCS(PROJECT_ID).client
        bucket = storage.Bucket(client, MY_TEST_BUCKET, user_project=PROJECT_ID)
        cls.bucket = GCSBucket(bucket)

    def test_list_files(self):
        blobs = self.bucket.list_files()
        assert isinstance(blobs, list)
        assert all([isinstance(blob, str) for blob in blobs])

    def test_get_file(self):
        blobs = self.bucket.list_files()
        blob = self.bucket.get_file(blobs[0])
        assert isinstance(blob, storage.blob.Blob)

    def test_file_exists(self):
        # check file that exists
        assert self.bucket.file_exists("211102_rabo_all_aois.geojson")
        # check file that does not exist
        assert not self.bucket.file_exists("non_existent_file.geojson")

    def test_upload_file(self):
        # Create a local file to upload
        test_file_name = f"test-file-{uuid.uuid4()}.txt"
        test_file_content = "This is a test file."
        with open(test_file_name, "w") as f:
            f.write(test_file_content)

        self.bucket.upload_file(
            test_file_name, f"test-upload-gcs-bucket/{test_file_name}"
        )

    @pytest.mark.e2e
    def test_download_single_file(self):
        blobs = self.bucket.list_files()
        blob = self.bucket.get_file(blobs[0])
        download_path = f"tests/delete-downloaded-{blob.name}"
        self.bucket.download_file(blob.name, download_path)
        assert os.path.exists(download_path)
        os.remove(download_path)

    @pytest.mark.e2e
    def test_download_directory(self):
        download_path = "tests/data/root3"
        self.bucket.download_file("root3/", download_path)
        assert os.path.exists(download_path)
        assert os.listdir(download_path) == [
            "subdir",
            "test-file-1.txt",
            "test-file-2.txt",
        ]
        assert os.listdir(f"{download_path}/subdir") == ["test-file-3.txt"]
        shutil.rmtree(download_path)

    def test_delete_file(self, test_file):
        self.bucket.upload_file(str(test_file), str(test_file))
        self.bucket.delete_file(str(test_file))
        assert str(test_file) not in self.bucket.list_files()


@pytest.mark.mock
def test_search():
    # Mock bucket and blobs
    mock_bucket = MagicMock()
    # Create mock blobs with specific names
    mock_blob1 = MagicMock()
    mock_blob1.name = "file1.txt"
    mock_blob2 = MagicMock()
    mock_blob2.name = "data/file2.txt"
    mock_blob3 = MagicMock()
    mock_blob3.name = "data/file3.log"
    mock_blob4 = MagicMock()
    mock_blob4.name = "logs/log1.txt"
    mock_bucket.list_blobs.return_value = [
        mock_blob1,
        mock_blob2,
        mock_blob3,
        mock_blob4,
    ]

    gcs_bucket = GCSBucket(mock_bucket)

    matching_files = gcs_bucket.search("*.txt")
    assert matching_files == ["file1.txt", "data/file2.txt", "logs/log1.txt"]

    # Test glob_files within a directory
    matching_files = gcs_bucket.search("*.txt", directory="data")
    assert matching_files == ["data/file2.txt"]


class TestDownloadMock:

    @pytest.mark.mock
    def test_download_directory_from_gcs(self):

        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob1.name = "test_dir/file1.txt"
        mock_blob2 = MagicMock()
        mock_blob2.name = "test_dir/subdir/file2.txt"
        mock_blob3 = MagicMock()
        mock_blob3.name = "test_dir/subdir/file3.txt"

        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

        gcs_bucket = GCSBucket(mock_bucket)

        mock_blob1.download_to_filename = MagicMock()
        mock_blob2.download_to_filename = MagicMock()
        mock_blob3.download_to_filename = MagicMock()

        local_path = Path("tests/data/local_test_dir")
        gcs_bucket.download_file("test_dir/", local_path)

        mock_blob1.download_to_filename.assert_called_once_with(
            local_path / "file1.txt"
        )
        mock_blob2.download_to_filename.assert_called_once_with(
            local_path / "subdir/file2.txt"
        )
        mock_blob3.download_to_filename.assert_called_once_with(
            local_path / "subdir/file3.txt"
        )

        assert (local_path / "subdir").exists()

        shutil.rmtree(local_path)

    @pytest.mark.mock
    def test_download_single_file_from_gcs(self):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.name = "test_file.txt"
        mock_bucket.blob.return_value = mock_blob

        gcs_bucket = GCSBucket(mock_bucket)

        mock_blob.download_to_filename = MagicMock()

        file_name = "test_file.txt"
        local_path = Path("local_test_file.txt")
        gcs_bucket.download_file(file_name, str(local_path))

        mock_bucket.blob.assert_called_once_with(file_name)
        mock_blob.download_to_filename.assert_called_once_with(str(local_path))

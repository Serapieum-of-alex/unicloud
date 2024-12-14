import os
import shutil
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, patch

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
        assert self.bucket.file_exists("211102_rabo_all_aois.geojson")
        assert not self.bucket.file_exists("non_existent_file.geojson")

    def test_upload_file(self, test_file: Path):
        bucket_path = f"test-upload-gcs-bucket-{test_file.name}"
        self.bucket.upload_file(test_file, bucket_path)
        assert any(blob.name == bucket_path for blob in self.bucket.bucket.list_blobs())
        self.bucket.delete(bucket_path)

    def test_upload_directory_with_subdirectories_e2e(
        self, upload_test_data: Dict[str, Path]
    ):
        local_dir = upload_test_data["local_dir"]
        bucket_path = upload_test_data["bucket_path"]

        self.bucket.upload_file(local_dir, bucket_path)

        uploaded_files = [blob.name for blob in self.bucket.bucket.list_blobs()]
        expected_files = upload_test_data["expected_files"]
        assert set(uploaded_files) & expected_files == expected_files

        # Cleanup
        for blob_name in list(expected_files):
            self.bucket.bucket.blob(blob_name).delete()

    @pytest.mark.e2e
    def test_download_single_file(self):
        blobs = self.bucket.list_files()
        blob = self.bucket.get_file(blobs[0])
        download_path = f"tests/delete-downloaded-{blob.name}"
        self.bucket.download(blob.name, download_path)
        assert os.path.exists(download_path)
        os.remove(download_path)

    @pytest.mark.e2e
    def test_download_directory(self):
        download_path = "tests/data/root3"
        self.bucket.download("root3/", download_path)
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
        self.bucket.delete(str(test_file))
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


class TestDeleteE2E:

    @pytest.fixture
    def gcs_bucket(self) -> GCSBucket:
        return GCS(PROJECT_ID).get_bucket(MY_TEST_BUCKET)

    def test_delete_single_file_e2e(self, gcs_bucket: GCSBucket):

        blob = gcs_bucket.bucket.blob("test_delete_single.txt")
        blob.upload_from_string("Test content")
        assert gcs_bucket.file_exists("test_delete_single.txt")

        gcs_bucket.delete("test_delete_single.txt")
        assert not gcs_bucket.file_exists("test_delete_single.txt")

    def test_delete_directory_e2e(self, gcs_bucket: GCSBucket):

        blob1 = gcs_bucket.bucket.blob("test_directory/file1.txt")
        blob1.upload_from_string("File 1 content")
        blob2 = gcs_bucket.bucket.blob("test_directory/subdir/file2.txt")
        blob2.upload_from_string("File 2 content")

        assert gcs_bucket.file_exists("test_directory/file1.txt")
        assert gcs_bucket.file_exists("test_directory/subdir/file2.txt")

        # Delete the directory
        gcs_bucket.delete("test_directory/")

        # Verify files are deleted
        assert not gcs_bucket.file_exists("test_directory/file1.txt")
        assert not gcs_bucket.file_exists("test_directory/subdir/file2.txt")

    def test_delete_nonexistent_file_e2e(self, gcs_bucket: GCSBucket):
        """Test deleting a file that does not exist."""
        with pytest.raises(
            ValueError, match="File non_existent_file.txt not found in the bucket."
        ):
            gcs_bucket.delete("non_existent_file.txt")

    def test_delete_empty_directory_e2e(self, gcs_bucket: GCSBucket):
        """Test deleting an empty directory."""
        directory = "empty_directory/"
        with pytest.raises(
            ValueError, match=f"No files found in the directory: {directory}"
        ):
            gcs_bucket.delete(directory)


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
        gcs_bucket.download("test_dir/", local_path)

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
        gcs_bucket.download(file_name, str(local_path))

        mock_bucket.blob.assert_called_once_with(file_name)
        mock_blob.download_to_filename.assert_called_once_with(str(local_path))


class TestUploadMock:

    def test_upload_single_file(self):

        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        gcs_bucket = GCSBucket(mock_bucket)

        local_file = Path("local/file.txt")
        bucket_path = "bucket/folder/file.txt"

        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.is_file", return_value=True
        ):
            gcs_bucket.upload_file(local_file, bucket_path)

        mock_bucket.blob.assert_called_once_with(bucket_path)
        mock_blob.upload_from_filename.assert_called_once_with(str(local_file))

    def test_upload_directory_with_subdirectories(self):

        mock_bucket = MagicMock()
        gcs_bucket = GCSBucket(mock_bucket)

        # Mock directory and files
        local_directory = Path("local/directory")
        bucket_path = "bucket/folder"
        files = [
            local_directory / "file1.txt",
            local_directory / "subdir" / "file2.txt",
            local_directory / "subdir" / "file3.log",
        ]

        # Mock rglob and existence checks
        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("pathlib.Path.is_dir", return_value=True),
            patch("pathlib.Path.rglob", return_value=files),
        ):

            # Mock individual file checks and uploads
            with patch("pathlib.Path.is_file", side_effect=[False, True, True, True]):
                gcs_bucket.upload_file(local_directory, bucket_path)

        for file in files:
            relative_path = file.relative_to(local_directory)
            bucket_file_path = f"{bucket_path}/{relative_path.as_posix()}"
            mock_bucket.blob.assert_any_call(bucket_file_path)
            mock_bucket.blob(bucket_file_path).upload_from_filename.assert_any_call(
                str(file)
            )


class TestDeleteMock:

    @pytest.mark.mock
    def test_delete_single_file(self):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True

        gcs_bucket = GCSBucket(mock_bucket)

        file_name = "example.txt"
        gcs_bucket.delete(file_name)

        mock_bucket.blob.assert_called_once_with(file_name)
        mock_blob.delete.assert_called_once()

    @pytest.mark.mock
    def test_delete_single_file_not_found(self):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False

        gcs_bucket = GCSBucket(mock_bucket)

        file_name = "non_existent_file.txt"
        with pytest.raises(
            ValueError, match=f"File {file_name} not found in the bucket."
        ):
            gcs_bucket.delete(file_name)

        mock_bucket.blob.assert_called_once_with(file_name)
        mock_blob.delete.assert_not_called()

    @pytest.mark.mock
    def test_delete_directory(self):
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob1.name = "data/file1.txt"
        mock_blob2 = MagicMock()
        mock_blob2.name = "data/subdir/file2.txt"
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]

        gcs_bucket = GCSBucket(mock_bucket)

        directory = "data/"
        gcs_bucket.delete(directory)

        mock_bucket.list_blobs.assert_called_once_with(prefix=directory)
        mock_blob1.delete.assert_called_once()
        mock_blob2.delete.assert_called_once()

    @pytest.mark.mock
    def test_delete_directory_empty(self):

        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = []

        gcs_bucket = GCSBucket(mock_bucket)

        directory = "empty_directory/"
        with pytest.raises(
            ValueError, match=f"No files found in the directory: {directory}"
        ):
            gcs_bucket.delete(directory)

        mock_bucket.list_blobs.assert_called_once_with(prefix=directory)

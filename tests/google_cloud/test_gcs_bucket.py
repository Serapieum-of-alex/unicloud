import fnmatch
import os
import shutil
import unittest
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, call, patch

import pytest
from google.cloud import storage

from unicloud.google_cloud.gcs import GCS, Bucket

MY_TEST_BUCKET = "testing-repositories"
PROJECT_ID = "earth-engine-415620"


class TestGCSBucketE2E:

    @classmethod
    def setup_class(cls):
        """Set up resources before running tests."""
        client = GCS(PROJECT_ID).client
        bucket = storage.Bucket(client, MY_TEST_BUCKET, user_project=PROJECT_ID)
        cls.bucket = Bucket(bucket)

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
        """
        Test uploading a single file to the bucket.
        """
        bucket_path = f"test-upload-gcs-bucket-{test_file.name}"
        self.bucket.upload(test_file, bucket_path)
        assert any(blob.name == bucket_path for blob in self.bucket.bucket.list_blobs())
        self.bucket.delete(bucket_path)

    def test_upload_directory_with_subdirectories_e2e(
        self, upload_test_data: Dict[str, Path]
    ):
        local_dir = upload_test_data["local_dir"]
        bucket_path = upload_test_data["bucket_path"]

        self.bucket.upload(local_dir, bucket_path)

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
        self.bucket.download(blob.name, download_path, overwrite=True)
        assert os.path.exists(download_path)
        os.remove(download_path)

    @pytest.mark.e2e
    def test_download_directory(self):
        dir_files = ["subdir", "test-file-1.txt", "test-file-2.txt"]
        download_path = "tests/data/root3"
        self.bucket.download("root3/", download_path, overwrite=True)
        assert os.path.exists(download_path)
        assert all(elem in os.listdir(download_path) for elem in dir_files)
        assert os.listdir(f"{download_path}/subdir") == ["test-file-3.txt"]
        shutil.rmtree(download_path)

    def test_delete_file(self, test_file):
        self.bucket.upload(str(test_file), str(test_file))
        self.bucket.delete(str(test_file))
        assert str(test_file) not in self.bucket.list_files()

    def test_rename_file(self, test_file: Path):
        """
        Test renaming a single file in the bucket.
        """
        old_name = "test-rename-old-file.txt"
        new_name = "test-rename-new-file.txt"
        self.bucket.upload(test_file, old_name, overwrite=True)

        self.bucket.rename(old_name, new_name)

        # Verify the new file exists and the old file does not
        assert self.bucket.file_exists(new_name)
        assert not self.bucket.file_exists(old_name)
        self.bucket.delete(new_name)

    def test_rename_directory(self, upload_test_data: Dict[str, Path]):
        """
        Test renaming a directory in the bucket.
        """
        old_dir = "old_directory/"
        new_dir = "new_directory/"
        local_dir = upload_test_data["local_dir"]
        self.bucket.upload(local_dir, old_dir, overwrite=True)

        # Rename the directory
        self.bucket.rename(old_dir, new_dir)

        # Verify files under the new directory exist and old directory does not
        for file in upload_test_data["expected_files"]:
            new_file = file.replace("upload-dir", "new_directory")
            assert self.bucket.file_exists(new_file)
            assert not self.bucket.file_exists(file)

        self.bucket.delete(new_dir)


@pytest.mark.mock
class TestListFilesMock:

    def setup_method(self):
        # Mock bucket and blobs
        self.mock_bucket = MagicMock()
        self.mock_blob1 = MagicMock()
        self.mock_blob1.name = "file1.txt"
        self.mock_blob2 = MagicMock()
        self.mock_blob2.name = "data/file2.csv"
        self.mock_blob3 = MagicMock()
        self.mock_blob3.name = "data/file3.log"
        self.mock_blob4 = MagicMock()
        self.mock_blob4.name = "logs/log1.txt"

        self.mock_bucket.list_blobs.return_value = [
            self.mock_blob1,
            self.mock_blob2,
            self.mock_blob3,
            self.mock_blob4,
        ]

        self.gcs_bucket = Bucket(self.mock_bucket)

    def test_list_all_files(self):
        """Test listing all files in the bucket."""
        files = self.gcs_bucket.list_files()
        assert files == [
            "file1.txt",
            "data/file2.csv",
            "data/file3.log",
            "logs/log1.txt",
        ]
        self.mock_bucket.list_blobs.assert_called_once_with(
            prefix=None, max_results=None
        )

    def test_list_files_with_prefix(self):
        """Test listing files with a prefix."""
        self.mock_bucket.list_blobs.side_effect = lambda prefix, max_results: [
            blob
            for blob in [self.mock_blob2, self.mock_blob3]
            if blob.name.startswith(prefix)
        ]

        # List files with prefix
        files = self.gcs_bucket.list_files(prefix="data/")
        assert files == ["data/file2.csv", "data/file3.log"]
        self.mock_bucket.list_blobs.assert_called_once_with(
            prefix="data/", max_results=None
        )

    def test_list_files_with_pattern(self):
        """Test listing files with a pattern."""
        files = self.gcs_bucket.list_files(pattern="*.txt")
        expected_files = [
            blob.name
            for blob in [self.mock_blob1, self.mock_blob4]
            if fnmatch.fnmatch(blob.name, "*.txt")
        ]
        assert files == expected_files

    def test_list_files_with_prefix_and_pattern(self):
        """Test listing files with a prefix and pattern."""
        self.mock_bucket.list_blobs.side_effect = lambda prefix, max_results: [
            blob
            for blob in [self.mock_blob2, self.mock_blob3]
            if blob.name.startswith(prefix)
        ]

        # List files with prefix and pattern
        files = self.gcs_bucket.list_files(prefix="data/", pattern="*.csv")
        expected_files = [
            blob.name
            for blob in [self.mock_blob2]
            if fnmatch.fnmatch(blob.name, "*.csv")
        ]
        assert files == expected_files

    def test_list_files_with_max_results(self):
        """Test listing files with a maximum number of results."""
        self.mock_bucket.list_blobs.side_effect = lambda prefix, max_results: [
            self.mock_blob1,
            self.mock_blob2,
        ][:max_results]

        # List files with max results
        files = self.gcs_bucket.list_files(max_results=2)
        assert files == ["file1.txt", "data/file2.csv"]
        self.mock_bucket.list_blobs.assert_called_once_with(prefix=None, max_results=2)

    def test_list_files_with_all_filters(self):
        """Test listing files with all filters."""
        self.mock_bucket.list_blobs.side_effect = lambda prefix, max_results: [
            blob
            for blob in [self.mock_blob2, self.mock_blob3]
            if blob.name.startswith(prefix)
        ][:max_results]

        # List files with prefix, pattern, and max results
        files = self.gcs_bucket.list_files(
            prefix="data/", pattern="*.log", max_results=2
        )
        expected_files = [
            blob.name
            for blob in [self.mock_blob3]
            if fnmatch.fnmatch(blob.name, "*.log")
        ][:1]
        assert files == expected_files


class TestDeleteE2E:
    """
    End-to-End tests for the Bucket class delete method.
    """

    @pytest.fixture
    def gcs_bucket(self) -> Bucket:
        return GCS(PROJECT_ID).get_bucket(MY_TEST_BUCKET)

    def test_delete_single_file_e2e(self, gcs_bucket: Bucket):

        blob = gcs_bucket.bucket.blob("test_delete_single.txt")
        blob.upload_from_string("Test content")
        assert gcs_bucket.file_exists("test_delete_single.txt")

        gcs_bucket.delete("test_delete_single.txt")
        assert not gcs_bucket.file_exists("test_delete_single.txt")

    def test_delete_directory_e2e(self, gcs_bucket: Bucket):

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

    def test_delete_nonexistent_file_e2e(self, gcs_bucket: Bucket):
        """Test deleting a file that does not exist."""
        with pytest.raises(
            ValueError, match="File non_existent_file.txt not found in the bucket."
        ):
            gcs_bucket.delete("non_existent_file.txt")

    def test_delete_empty_directory_e2e(self, gcs_bucket: Bucket):
        """Test deleting an empty directory."""
        directory = "empty_directory/"
        with pytest.raises(
            ValueError, match=f"No files found in the directory: {directory}"
        ):
            gcs_bucket.delete(directory)


class TestPropertiesMock:

    def setup_method(self):
        self.mock_bucket = MagicMock()
        self.mock_bucket.name = "test_bucket"
        self.gcs_bucket = Bucket(self.mock_bucket)

    def test_name_property(self):
        assert self.gcs_bucket.name == "test_bucket"

    def test__str__(self):
        assert str(self.gcs_bucket) == "Bucket: test_bucket"

    def test__repr__(self):
        assert str(self.gcs_bucket.__repr__()) == "Bucket: test_bucket"


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

        gcs_bucket = Bucket(mock_bucket)

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

        gcs_bucket = Bucket(mock_bucket)

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

        gcs_bucket = Bucket(mock_bucket)

        local_file = Path("local/file.txt")
        bucket_path = "bucket/folder/file.txt"

        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.is_file", return_value=True
        ):
            gcs_bucket.upload(local_file, bucket_path, overwrite=True)

        mock_bucket.blob.assert_called_once_with(bucket_path)
        mock_blob.upload_from_filename.assert_called_once_with(str(local_file))

    def test_upload_directory_with_subdirectories(self):

        mock_bucket = MagicMock()
        gcs_bucket = Bucket(mock_bucket)

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
            patch("pathlib.Path.iterdir", return_value=files),
            patch("pathlib.Path.rglob", return_value=files),
        ):
            # Mock individual file checks and uploads
            with patch("pathlib.Path.is_file", side_effect=[False, True, True, True]):
                gcs_bucket.upload(local_directory, bucket_path, overwrite=True)

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

        gcs_bucket = Bucket(mock_bucket)

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

        gcs_bucket = Bucket(mock_bucket)

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

        gcs_bucket = Bucket(mock_bucket)

        directory = "data/"
        gcs_bucket.delete(directory)

        mock_bucket.list_blobs.assert_called_once_with(prefix=directory)
        mock_blob1.delete.assert_called_once()
        mock_blob2.delete.assert_called_once()

    @pytest.mark.mock
    def test_delete_directory_empty(self):

        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = []

        gcs_bucket = Bucket(mock_bucket)

        directory = "empty_directory/"
        with pytest.raises(
            ValueError, match=f"No files found in the directory: {directory}"
        ):
            gcs_bucket.delete(directory)

        mock_bucket.list_blobs.assert_called_once_with(prefix=directory)


class TestGCSBucketRename(unittest.TestCase):
    def setUp(self):
        self.mock_bucket = MagicMock()
        self.bucket = Bucket(self.mock_bucket)

    def test_rename_file(self):
        # Mock the list_blobs method to return a single file
        old_blob = MagicMock(name="old_path/file.txt")
        self.mock_bucket.list_blobs.side_effect = [[old_blob], []]

        # Call the rename method
        self.bucket.rename("old_path/file.txt", "new_path/file.txt")

        # Verify the copy and delete operations
        self.mock_bucket.blob("new_path/file.txt").rewrite.assert_called_once_with(
            old_blob
        )
        old_blob.delete.assert_called_once()

    def test_rename_directory(self):
        # Mock the list_blobs method to return multiple files in a directory
        old_blob_1 = MagicMock(name="old_path/dir/file1.txt")
        old_blob_2 = MagicMock(name="old_path/dir/file2.txt")
        self.mock_bucket.list_blobs.side_effect = [[old_blob_1, old_blob_2], []]

        # Call the rename method
        self.bucket.rename("old_path/dir/", "new_path/dir/")

        # Verify the copy and delete operations for each file
        expected_calls = [call(old_blob_1), call(old_blob_2)]
        self.mock_bucket.blob("new_path/dir/file1.txt").rewrite.assert_has_calls(
            [expected_calls[0]]
        )
        self.mock_bucket.blob("new_path/dir/file2.txt").rewrite.assert_has_calls(
            [expected_calls[1]]
        )
        old_blob_1.delete.assert_called_once()
        old_blob_2.delete.assert_called_once()

    def test_rename_nonexistent_path(self):
        # Mock the list_blobs method to return an empty list
        self.mock_bucket.list_blobs.return_value = []

        # Verify that ValueError is raised
        with self.assertRaises(ValueError):
            self.bucket.rename("nonexistent_path/", "new_path/")

    def test_rename_to_existing_path(self):
        # Mock the list_blobs method to return a single file for both old and new paths
        self.mock_bucket.list_blobs.side_effect = [
            [MagicMock(name="old_path/file.txt")],
            [MagicMock(name="new_path/file.txt")],
        ]

        # Verify that ValueError is raised
        with self.assertRaises(ValueError):
            self.bucket.rename("old_path/file.txt", "new_path/file.txt")

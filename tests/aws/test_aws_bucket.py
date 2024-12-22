import shutil
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, patch

import boto3
import pytest

from unicloud.aws.aws import Bucket


class TestBucketE2E:
    """
    End-to-End tests for the Bucket class.
    """

    @pytest.fixture(autouse=True)
    def setup(self, s3_bucket_name, aws_access_key_id, aws_secret_access_key, region):
        """
        Setup a mock S3 bucket and temporary directory for testing.
        """
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
        )
        self.bucket = Bucket(s3.Bucket(s3_bucket_name))

    def test_upload_file(self, test_file: Path):
        """
        Test uploading a single file to the bucket.
        """
        file_name = "test-upload-file.txt"
        self.bucket.upload(test_file, file_name)
        objects = [obj.key for obj in self.bucket.bucket.objects.all()]
        assert file_name in objects
        self.bucket.delete(file_name)

    def test_upload_directory(self, upload_test_data: Dict[str, Path]):
        """
        Test uploading a directory to the bucket.
        """
        local_dir = upload_test_data["local_dir"]
        bucket_path = upload_test_data["bucket_path"]

        self.bucket.upload(local_dir, f"{bucket_path}/")
        objects = [obj.key for obj in self.bucket.bucket.objects.all()]
        expected_files = upload_test_data["expected_files"]
        assert set(objects) & expected_files == expected_files
        self.bucket.delete(f"{bucket_path}/")

    def test_upload_overwrite(self, test_file: Path):
        """
        Test uploading a file with overwrite behavior.
        """
        file_name = "test-upload-overwrite.txt"
        self.bucket.upload(test_file, file_name)

        # Overwrite = False
        with pytest.raises(ValueError, match="File .* already exists."):
            self.bucket.upload(test_file, file_name, overwrite=False)

        # Overwrite = True
        self.bucket.upload(test_file, file_name, overwrite=True)
        objects = [obj.key for obj in self.bucket.bucket.objects.all()]
        assert file_name in objects
        self.bucket.delete(file_name)

    def test_download_file(self, test_file: Path, test_file_content: str):
        """
        Test downloading a single file from the bucket.
        """
        file_name = "test-download-file.txt"
        self.bucket.upload(test_file, file_name, overwrite=True)
        download_path = Path("tests/data/aws-downloaded-file.txt")
        self.bucket.download(file_name, str(download_path))
        assert download_path.exists()
        assert download_path.read_text() == test_file_content
        self.bucket.delete(file_name)
        download_path.unlink()

    def test_download_directory(self, upload_test_data: Dict[str, Path]):
        """
        Test downloading a directory from the bucket.
        """
        local_dir = upload_test_data["local_dir"]
        bucket_path = "test-download-dir"
        expected_files = upload_test_data["expected_files"]

        self.bucket.upload(local_dir, f"{bucket_path}/", overwrite=True)

        download_path = Path("tests/data/aws-downloaded-dir")
        self.bucket.download(f"{bucket_path}/", str(download_path))

        expected_files = [
            file.replace("upload-dir", download_path.name) for file in expected_files
        ]
        assert download_path.exists()
        assert download_path.is_dir()

        actual_files = [
            str(file.relative_to(download_path.parent)).replace("\\", "/")
            for file in download_path.rglob("*")
            if file.is_file()
        ]
        assert set(actual_files) == set(expected_files)
        shutil.rmtree(download_path)


class TestUploadMock:
    """
    Mock tests for the Bucket class.
    """

    def setup_method(self):
        """
        Setup a mocked S3 Bucket instance.
        """
        self.mock_bucket = MagicMock()
        self.bucket = Bucket(self.mock_bucket)

    def test_upload_file(self):
        """
        Test uploading a single file to the bucket using mocks.
        """
        local_file = Path("test.txt")
        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.is_file", return_value=True
        ):
            self.bucket.upload(local_file, "test.txt")
        self.mock_bucket.upload_file.assert_called_once_with(
            Filename=str(local_file), Key="test.txt"
        )

    def test_upload_directory(self):
        """
        Test uploading a directory to the bucket using mocks.
        """
        local_dir = Path("test_dir")
        files = [local_dir / "file1.txt", local_dir / "file2.txt"]

        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.is_dir", return_value=True
        ), patch(
            "os.walk", return_value=[(str(local_dir), [], [f.name for f in files])]
        ):
            self.bucket.upload(local_dir, "test_dir/")

        for file in files:
            s3_path = f"test_dir/{file.name}"
            self.mock_bucket.upload_file.assert_any_call(
                Filename=str(file), Key=s3_path
            )


class TestDownloadMock:
    def setup_method(self):
        """
        Setup a mocked S3 Bucket instance.
        """
        self.mock_bucket = MagicMock()
        self.bucket = Bucket(self.mock_bucket)

    def test_download_file(self):
        """
        Test downloading a single file from the bucket using mocks.
        """
        local_file = Path("downloaded.txt")
        with patch("pathlib.Path.exists", return_value=False), patch(
            "pathlib.Path.mkdir"
        ):
            self.bucket.download("test.txt", str(local_file))

        self.mock_bucket.download_file.assert_called_once_with(
            Key="test.txt", Filename=str(local_file)
        )

    def test_download_directory(self):
        """
        Test downloading a directory from the bucket using mocks.
        """
        local_dir = Path("downloaded_dir")
        mock_objects = [
            MagicMock(key="test_dir/file1.txt"),
            MagicMock(key="test_dir/file2.txt"),
        ]
        self.mock_bucket.objects.filter.return_value = mock_objects

        with patch("pathlib.Path.mkdir") as mock_mkdir:
            self.bucket.download("test_dir/", str(local_dir))

        for obj in mock_objects:
            expected_path = local_dir / Path(obj.key).relative_to("test_dir/")
            self.mock_bucket.download_file.assert_any_call(
                Key=obj.key, Filename=str(expected_path)
            )


class TestDeleteE2E:
    """
    End-to-End tests for the Bucket class delete method.
    """

    @pytest.fixture(autouse=True)
    def setup(self, s3_bucket_name, aws_access_key_id, aws_secret_access_key, region):
        """
        Setup a mock S3 bucket and temporary directory for testing.
        """
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
        )
        self.bucket = Bucket(s3.Bucket(s3_bucket_name))

    def test_delete_file(self, test_file):
        """
        Test deleting a single file from the bucket.
        """
        file_name = "test-delete-file.txt"
        self.bucket.upload(test_file, file_name)
        self.bucket.delete(file_name)
        objects = [obj.key for obj in self.bucket.bucket.objects.all()]
        assert file_name not in objects

    def test_delete_directory(self, upload_test_data: Dict[str, Path]):
        """
        Test deleting a directory from the bucket.
        """
        local_dir = upload_test_data["local_dir"]
        bucket_path = upload_test_data["bucket_path"]

        self.bucket.upload(local_dir, f"{bucket_path}/")
        self.bucket.delete(f"{bucket_path}/")

        objects = self.bucket.list_files(f"{bucket_path}/")
        assert not objects

    def test_delete_empty_directory(self):
        """
        Test attempting to delete an empty directory in the bucket.
        """
        empty_dir = "empty-dir/"
        with pytest.raises(
            ValueError, match=f"No files found in the directory: {empty_dir}"
        ):
            self.bucket.delete(empty_dir)

    def test_delete_nonexistent_file(self):
        """
        Test attempting to delete a nonexistent file in the bucket.
        """
        nonexistent_file = "nonexistent-file.txt"
        with pytest.raises(
            ValueError, match=f"File {nonexistent_file} not found in the bucket."
        ):
            self.bucket.delete(nonexistent_file)


class TestDeleteMock:

    def setup_method(self):
        """
        Setup a mocked S3 Bucket instance.
        """
        self.mock_bucket = MagicMock()
        self.bucket = Bucket(self.mock_bucket)

    def test_delete_file(self):
        """
        Test deleting a single file from the bucket using mocks.
        """
        object_mock = MagicMock()
        object_mock.key = "test.txt"
        self.mock_bucket.objects.filter.return_value = [object_mock]
        self.bucket.delete("test.txt")
        self.mock_bucket.Object.return_value.delete.assert_called_once()

    def test_delete_directory(self):
        """
        Test deleting a directory from the bucket using mocks.
        """
        mock_objects = [
            MagicMock(key="test_dir/file1.txt"),
            MagicMock(key="test_dir/file2.txt"),
        ]
        self.mock_bucket.objects.filter.return_value = mock_objects
        self.bucket.delete("test_dir/")
        for obj in mock_objects:
            obj.delete.assert_called_once()

    def test_delete_empty_directory_mock(self):
        """
        Test deleting an empty directory using mocks.
        """
        self.mock_bucket.objects.filter.return_value = []
        with pytest.raises(
            ValueError, match="No files found in the directory: empty-dir/"
        ):
            self.bucket.delete("empty-dir/")

    def test_delete_nonexistent_file_mock(self):
        """
        Test deleting a nonexistent file using mocks.
        """
        self.mock_bucket.objects.filter.return_value = []

        with pytest.raises(
            ValueError, match="File nonexistent-file.txt not found in the bucket."
        ):
            self.bucket.delete("nonexistent-file.txt")

        self.mock_bucket.objects.filter.assert_called_once_with(
            Prefix="nonexistent-file.txt"
        )

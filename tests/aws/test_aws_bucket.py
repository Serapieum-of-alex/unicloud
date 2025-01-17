import os
import shutil
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, call, patch

import boto3
import pytest

from unicloud.aws.aws import Bucket


class TestBucketE2E:
    """
    End-to-End tests for the Bucket class.
    """

    @pytest.fixture(autouse=True)
    def setup(self, s3_bucket_name):
        """
        Setup a mock S3 bucket and temporary directory for testing.
        """
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION"),
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

        # test with Overwrite = False
        with pytest.raises(ValueError, match="File .* already exists."):
            self.bucket.upload(test_file, file_name, overwrite=False)

        # test with Overwrite = True
        self.bucket.upload(test_file, file_name, overwrite=True)
        objects = [obj.key for obj in self.bucket.bucket.objects.all()]
        assert file_name in objects
        self.bucket.delete(file_name)

    def test_upload_empty_directory(self):
        """
        Test uploading an empty directory to the bucket.
        """
        empty_dir = Path("tests/data/empty-dir")
        empty_dir.mkdir(parents=True, exist_ok=True)
        with pytest.raises(ValueError, match="Directory .* is empty."):
            self.bucket.upload(empty_dir, "empty-dir/")
        shutil.rmtree(empty_dir)

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

    def test_download_overwrite(self, test_file: Path):
        """
        Test downloading a file with overwrite behavior.
        """
        file_name = "test-download-overwrite.txt"
        download_path = Path("tests/data/aws-downloaded-file.txt")

        self.bucket.upload(test_file, file_name)
        self.bucket.download(file_name, str(download_path))

        # test with Overwrite = False
        with pytest.raises(ValueError, match="File .* already exists locally."):
            self.bucket.download(file_name, str(download_path), overwrite=False)

        # test with Overwrite = True
        self.bucket.download(file_name, str(download_path), overwrite=True)
        self.bucket.delete(file_name)
        download_path.unlink()

    def test_download_empty_directory(self):
        """
        Test downloading an empty directory from the bucket.
        """
        with pytest.raises(ValueError, match="Directory .* is empty."):
            self.bucket.download("empty-dir/", "tests/data/empty-dir/")

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
        ), patch("pathlib.Path.iterdir", return_value=files), patch(
            "os.walk", return_value=[(str(local_dir), [], [f.name for f in files])]
        ):
            self.bucket.upload(local_dir, "test_dir/")

        for file in files:
            s3_path = f"test_dir/{file.name}"
            self.mock_bucket.upload_file.assert_any_call(
                Filename=str(file), Key=s3_path
            )

    def test_upload_empty_directory_mock(self):
        """
        Test uploading an empty directory to the bucket using mocks.
        """
        empty_dir = Path("test-empty-dir")
        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.is_dir", return_value=True
        ), patch("pathlib.Path.iterdir", return_value=[]):
            with pytest.raises(ValueError, match="Directory .* is empty."):
                self.bucket.upload(empty_dir, "empty-dir/")


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

        with patch("pathlib.Path.mkdir"):
            self.bucket.download("test_dir/", str(local_dir))

        for obj in mock_objects:
            expected_path = local_dir / Path(obj.key).relative_to("test_dir/")
            self.mock_bucket.download_file.assert_any_call(
                Key=obj.key, Filename=str(expected_path)
            )

    def test_download_empty_directory_mock(self):
        """
        Test downloading an empty directory using mocks.
        """
        with patch("pathlib.Path.mkdir"), patch(
            "unicloud.aws.aws.Bucket.list_files", return_value=[]
        ):
            with pytest.raises(ValueError, match="Directory .* is empty."):
                self.bucket.download("empty-dir/", "local-empty-dir/")


class TestDeleteE2E:
    """
    End-to-End tests for the Bucket class delete method.
    """

    @pytest.fixture(autouse=True)
    def setup(self, s3_bucket_name):
        """
        Setup a mock S3 bucket and temporary directory for testing.
        """
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION"),
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


class TestRenameMock:
    def setup_method(self):
        """
        Setup a mocked S3 Bucket instance.
        """
        # the aws original bucket
        self.mock_bucket = MagicMock()
        # my bucket object
        self.bucket = Bucket(self.mock_bucket)

    def test_rename_file_mock(self):
        """
        Test renaming a file in the bucket using mocks.
        """
        mock_obj = MagicMock()
        mock_obj.key = "folder/old_file.txt"
        # the aws original bucket objects
        self.mock_bucket.objects.filter.side_effect = [[mock_obj], []]
        self.bucket.rename("folder/old_file.txt", "folder/new_file.txt")

        # Verify copy and delete
        self.mock_bucket.Object(
            "folder/new_file.txt"
        ).copy_from.assert_called_once_with(
            CopySource={"Bucket": self.mock_bucket.name, "Key": "folder/old_file.txt"}
        )
        mock_obj.delete.assert_called_once()

    def test_rename_directory_mock(self):
        """
        Test renaming a directory in the bucket using mocks.
        """
        mock_obj1 = MagicMock(key="folder/old_dir/file1.txt")
        mock_obj2 = MagicMock(key="folder/old_dir/file2.txt")
        self.mock_bucket.objects.filter.side_effect = [[mock_obj1, mock_obj2], []]

        self.bucket.rename("folder/old_dir/", "folder/new_dir/")

        expected_calls = [
            call(
                CopySource={
                    "Bucket": self.mock_bucket.name,
                    "Key": "folder/old_dir/file1.txt",
                }
            ),
            call(
                CopySource={
                    "Bucket": self.mock_bucket.name,
                    "Key": "folder/old_dir/file2.txt",
                }
            ),
        ]
        self.mock_bucket.Object("folder/new_dir/file1.txt").copy_from.assert_has_calls(
            [expected_calls[0]]
        )
        self.mock_bucket.Object("folder/new_dir/file2.txt").copy_from.assert_has_calls(
            [expected_calls[1]]
        )
        mock_obj1.delete.assert_called_once()
        mock_obj2.delete.assert_called_once()

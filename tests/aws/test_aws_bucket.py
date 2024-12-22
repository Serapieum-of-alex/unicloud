import shutil
from pathlib import Path
from typing import Dict

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

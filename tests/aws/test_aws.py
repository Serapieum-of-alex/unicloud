"""This module contains tests for the S3 class in unicloud/aws.py."""

import os
from pathlib import Path

import boto3
from moto import mock_aws

from unicloud.aws.aws import S3, Bucket

MY_TEST_BUCKET = "testing-unicloud"
MOCK_BUCKET_NAME = "testing-fake-name"


class TestS3Mock:
    """Test the S3 class."""

    def setup_method(self):
        """Set up the S3 client."""
        self.mock = mock_aws()
        self.mock.start()

        self.my_s3 = S3()

        # Create a mock S3 bucket
        self.bucket_name = MOCK_BUCKET_NAME
        self.my_s3.client.create_bucket(
            Bucket=self.bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": os.getenv("AWS_DEFAULT_REGION", "us-east-1")
            },
        )

    def teardown_method(self):
        """Stop the S3 mock."""
        self.mock.stop()

    def test_upload(self, test_file: str):
        """Test uploading data to S3."""
        bucket_name = MOCK_BUCKET_NAME
        object_name = "test-object"
        destination = f"{bucket_name}/{object_name}"

        self.my_s3.upload(test_file, destination)

        # Check the file exists in the bucket
        response = self.my_s3.client.list_objects_v2(Bucket=self.bucket_name)
        object_keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert object_name in object_keys

    def test_download_data(self, test_file: str, test_file_content: str):
        """Test downloading data from S3."""

        bucket_name = MOCK_BUCKET_NAME
        object_name = "test-object.txt"
        bucket_path = f"{bucket_name}/{object_name}"
        # Manually upload a file to mock S3 to download later
        self.my_s3.client.put_object(
            Bucket=self.bucket_name, Key=object_name, Body=test_file_content
        )
        download_path = "tests/data/test-download-aws.txt"
        self.my_s3.download(bucket_path, download_path)

        # Verify the file was downloaded correctly
        with open(download_path, "r") as f:
            assert f.read() == test_file_content


class TestS3E2E:
    """End-to-end tests for the S3 class."""

    file_name = "test_upload.txt"

    def test_s3_upload(self, unicloud_s3, test_file: Path, boto_client: boto3.client):
        """Test file upload to S3."""

        unicloud_s3.upload(test_file, f"{MY_TEST_BUCKET}/{self.file_name}")
        # Verify the file exists in S3
        response = boto_client.list_objects_v2(Bucket=MY_TEST_BUCKET)
        assert self.file_name in [obj["Key"] for obj in response["Contents"]]

    def test_s3_download(self, unicloud_s3, test_file_content: str):
        """Test file download from S3."""

        download_path = Path("tests/data/aws-test-file.txt")
        unicloud_s3.download(f"{MY_TEST_BUCKET}/{self.file_name}", download_path)

        # Verify the file content
        assert download_path.read_text() == test_file_content
        os.remove(download_path)

    def test_get_bucket(self, unicloud_s3):
        """Test getting a bucket object."""
        bucket = unicloud_s3.get_bucket(MY_TEST_BUCKET)
        assert bucket.bucket.name == MY_TEST_BUCKET
        assert isinstance(bucket, Bucket)

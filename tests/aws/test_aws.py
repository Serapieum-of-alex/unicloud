"""This module contains tests for the S3 class in unicloud/aws.py."""

import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from unicloud.aws.aws import S3, Bucket

MY_TEST_BUCKET = "testing-unicloud"
AWS_ACCESS_KEY_ID = os.getenv("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = os.getenv("aws_secret_access_key")
REGION = "eu-central-1"


@pytest.fixture
def boto_client() -> boto3.client:
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION,
    )



class TestS3:
    """Test the S3 class."""

    def setup_method(self):
        """Set up the S3 client."""
        mock = mock_aws()
        mock.start()

        """Setup for S3 client tests."""
        self.aws_access_key_id = "fake_key"
        self.aws_secret_access_key = "fake_secret"
        self.region_name = "us-east-1"
        self.client = S3(
            self.aws_access_key_id, self.aws_secret_access_key, self.region_name
        )

        # Create a mock S3 bucket
        self.bucket_name = MY_TEST_BUCKET
        self.client.client.create_bucket(Bucket=self.bucket_name)

    def test_upload(self, test_file: str):
        """Test uploading data to S3."""
        bucket_name = MY_TEST_BUCKET
        object_name = "test-object"
        destination = f"{bucket_name}/{object_name}"

        self.client.upload(test_file, destination)

        # Check the file exists in the bucket
        response = self.client.client.list_objects_v2(Bucket=self.bucket_name)
        object_keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert object_name in object_keys

    def test_download_data(self, test_file: str, test_file_content: str):
        """Test downloading data from S3."""

        bucket_name = MY_TEST_BUCKET
        object_name = "test-object.txt"
        bucket_path = f"{bucket_name}/{object_name}"
        # Manually upload a file to mock S3 to download later
        self.client.client.put_object(
            Bucket=self.bucket_name, Key=object_name, Body=test_file_content
        )
        download_path = "tests/data/test-download-aws.txt"
        self.client.download(bucket_path, download_path)

        # Verify the file was downloaded correctly
        with open(download_path, "r") as f:
            assert f.read() == test_file_content


class TestS3E2E:
    """End-to-end tests for the S3 class."""

    s3 = S3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION)
    file_name = "test_upload.txt"

    def test_s3_upload(self, test_file: Path, boto_client: boto3.client):
        """Test file upload to S3."""

        self.s3.upload(test_file, f"{MY_TEST_BUCKET}/{self.file_name}")
        # Verify the file exists in S3
        response = boto_client.list_objects_v2(Bucket=MY_TEST_BUCKET)
        assert self.file_name in [obj["Key"] for obj in response["Contents"]]

    def test_s3_download(self, test_file_content: str):
        """Test file download from S3."""

        download_path = Path("tests/data/aws-test-file.txt")
        self.s3.download(f"{MY_TEST_BUCKET}/{self.file_name}", download_path)

        # Verify the file content
        assert download_path.read_text() == test_file_content
        os.remove(download_path)

    def test_get_bucket(self):
        """Test getting a bucket object."""
        bucket = self.s3.get_bucket(MY_TEST_BUCKET)
        assert bucket.bucket.name == MY_TEST_BUCKET
        assert isinstance(bucket, Bucket)

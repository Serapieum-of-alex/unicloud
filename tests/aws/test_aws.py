"""This module contains tests for the S3 class in unicloud/aws.py."""

import boto3
import pytest
from moto import mock_aws

from unicloud.aws.aws import S3

MY_TEST_BUCKET = "test-bucket"


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


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    return {
        "aws_access_key_id": "testing",
        "aws_secret_access_key": "testing",
        "region_name": "us-east-1",
    }


@pytest.fixture
def s3_client(aws_credentials):
    """Create an S3 client for testing."""
    with mock_aws():
        boto3.client("s3", region_name=aws_credentials["region_name"]).create_bucket(
            Bucket="my-test-bucket"
        )
        yield S3(**aws_credentials)


class TestS3E2E:
    """End-to-end tests for the S3 class."""

    def test_s3_upload(self, s3_client, tmp_path):
        """Test file upload to S3."""
        # Create a temporary file to upload
        file_path = tmp_path / "test_upload.txt"
        file_path.write_text("Hello, world!")

        s3_client.upload(str(file_path), f"{MY_TEST_BUCKET}/test_upload.txt")

        # Verify the file exists in S3
        s3 = boto3.client("s3", region_name="us-east-1")
        response = s3.list_objects_v2(Bucket=MY_TEST_BUCKET)
        assert "test_upload.txt" in [obj["Key"] for obj in response["Contents"]]

    def test_s3_download(self, s3_client, tmp_path):
        """Test file download from S3."""
        # Assuming the file "test_upload.txt" already exists in S3 from the previous test
        download_path = tmp_path / "downloaded_test.txt"
        s3_client.download(f"{MY_TEST_BUCKET}/test_upload.txt", str(download_path))

        # Verify the file content
        assert download_path.read_text() == "Hello, world!"

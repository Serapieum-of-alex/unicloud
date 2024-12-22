from pathlib import Path
from typing import Dict

import boto3
import pytest

from unicloud.aws.aws import Bucket


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

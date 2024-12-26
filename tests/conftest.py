import os
from pathlib import Path
from typing import Dict

import boto3
import pytest

from unicloud.aws.aws import S3

S3_BUCKET_NAME = "testing-unicloud"


@pytest.fixture
def s3_bucket_name() -> str:
    return S3_BUCKET_NAME


@pytest.fixture
def unicloud_s3() -> S3:
    return S3()


@pytest.fixture
def test_file() -> Path:
    return Path("tests/data/test-file.txt")


@pytest.fixture
def test_file_content() -> str:
    return "This is a test file.\n"


@pytest.fixture
def upload_test_data() -> Dict[str, Path]:
    local_dir = Path("tests/data/upload-dir")
    bucket_path = "upload-dir"
    expected_files = {
        f"{bucket_path}/file1.txt",
        f"{bucket_path}/subdir/file2.txt",
        f"{bucket_path}/subdir/file3.log",
    }
    return {
        "local_dir": local_dir,
        "bucket_path": bucket_path,
        "expected_files": expected_files,
    }


@pytest.fixture
def boto_client() -> boto3.client:
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    return {
        "aws_access_key_id": "testing",
        "aws_secret_access_key": "testing",
        "region_name": "us-east-1",
    }

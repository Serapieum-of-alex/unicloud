import os
from pathlib import Path
from typing import Dict

import boto3
import pytest

from unicloud.aws.aws import S3

AWS_ACCESS_KEY_ID = os.getenv("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = os.getenv("aws_secret_access_key")
REGION = "eu-central-1"


@pytest.fixture
def unicloud_s3() -> S3:
    return S3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION)


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
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION,
    )
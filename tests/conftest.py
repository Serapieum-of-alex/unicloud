import os
from pathlib import Path
from typing import Dict

import pytest


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

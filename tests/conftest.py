from pathlib import Path

import pytest


@pytest.fixture
def test_file() -> Path:
    return Path("tests/data/test-file.txt")


@pytest.fixture
def test_file_content() -> str:
    return "This is a test file.\n"

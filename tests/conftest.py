import shutil
from pathlib import Path

import pytest

from .utils import TESTS_DIR, find_free_port


@pytest.fixture
def tests_dir() -> Path:
    return TESTS_DIR


@pytest.fixture
def bufferdir(tmp_path: Path) -> Path:
    target = tmp_path / "bufferdir"
    shutil.copytree(TESTS_DIR / "bufferdir", target)
    return target


@pytest.fixture
def lorem_text() -> str:
    return (TESTS_DIR / "lorem_ipsum.txt").read_text()


@pytest.fixture
def lorem_checksum() -> str:
    return "2d8c2f6d978ca21712b5f6de36c9d31fa8e96a4fa5d8ff8b0188dfb9e7c171bb"


@pytest.fixture
def available_port() -> int:
    return find_free_port()

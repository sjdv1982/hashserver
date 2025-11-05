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
    return "bde3f269175e1dcda13848278aa6046bd643cea85b84c8b8bb80952e70b6eae0"


@pytest.fixture
def available_port() -> int:
    return find_free_port()

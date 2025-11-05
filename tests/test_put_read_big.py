import time
from hashlib import sha3_256

import requests

from .utils import start_server, wait_for_server


CHUNK = b"0123456789abcdef" * 64 * 1024
BUFFER = CHUNK * 5  # ~5 MiB


def calculate_checksum(buffer: bytes) -> str:
    return sha3_256(buffer).digest().hex()


def test_put_read_big(tmp_path, available_port):
    write_dir = tmp_path / "writedir"
    write_dir.mkdir()

    port = available_port
    env = {
        "HASHSERVER_DIRECTORY": str(write_dir),
        "HASHSERVER_WRITABLE": "1",
    }
    command = [
        "uvicorn",
        "hashserver:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-level",
        "warning",
    ]

    with start_server(command, env=env) as _server:
        wait_for_server(port)

        start = time.monotonic()
        checksum = calculate_checksum(BUFFER)

        put_response = requests.put(
            f"http://127.0.0.1:{port}/{checksum}", data=BUFFER, timeout=60
        )
        assert put_response.status_code == 200, put_response.content
        assert put_response.content == b"OK"

        get_response = requests.get(
            f"http://127.0.0.1:{port}/{checksum}", timeout=60
        )
        assert get_response.status_code == 200, get_response.content
        assert get_response.content == BUFFER
        assert time.monotonic() - start < 30

import time
from hashlib import sha3_256

import requests

from .utils import start_server, wait_for_server


BUFFER = b"""This is a buffer
that is used
for testing purposes"""


def calculate_checksum(buffer: bytes) -> str:
    return sha3_256(buffer).digest().hex()


def get_buffer(port: int, checksum: str):
    response = requests.get(f"http://127.0.0.1:{port}/{checksum}", timeout=5)
    return response.status_code, response.content


def put_buffer(port: int, buffer: bytes):
    checksum = calculate_checksum(buffer)
    response = requests.put(
        f"http://127.0.0.1:{port}/{checksum}", data=buffer, timeout=5
    )
    return response.status_code, response.content


def test_put_and_read(tmp_path, available_port):
    write_dir = tmp_path / "writedir"
    write_dir.mkdir()

    port = available_port
    command = [
        "hashserver",
        str(write_dir),
        "--writable",
        "--layout",
        "flat",
        "--port",
        str(port),
    ]

    with start_server(command) as _server:
        wait_for_server(port)

        start = time.monotonic()
        status, body = put_buffer(port, BUFFER)
        assert status == 200, (status, body)
        assert body == b"OK", body
        assert time.monotonic() - start < 1

        checksum = calculate_checksum(BUFFER)

        start = time.monotonic()
        status, body = get_buffer(port, checksum)
        assert status == 200, (status, body)
        assert body == BUFFER, body
        assert time.monotonic() - start < 1

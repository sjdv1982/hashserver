import json
from hashlib import sha3_256

import requests

from .utils import start_server, wait_for_server


BUFFER = b"This is a test"


def calculate_checksum(buffer: bytes) -> str:
    return sha3_256(buffer).digest().hex()


def put_buffer(port: int, buffer: bytes):
    checksum = calculate_checksum(buffer)
    response = requests.put(
        f"http://127.0.0.1:{port}/{checksum}", data=buffer, timeout=5
    )
    return response.status_code, response.content


def bugged_put_truncated_checksum(port: int, buffer: bytes):
    checksum = calculate_checksum(buffer)
    response = requests.put(
        f"http://127.0.0.1:{port}/{checksum[:10]}", params={"buffer": buffer}, timeout=5
    )
    return response.status_code, response.content


def bugged_put_partial_buffer(port: int, buffer: bytes):
    checksum = calculate_checksum(buffer)
    response = requests.put(
        f"http://127.0.0.1:{port}/{checksum}", params={"buffer": buffer[:10]}, timeout=5
    )
    return response.status_code, response.content


def bugged_put_wrong_param(port: int, buffer: bytes):
    checksum = calculate_checksum(buffer)
    response = requests.put(
        f"http://127.0.0.1:{port}/{checksum}", params={"buf": buffer}, timeout=5
    )
    return response.status_code, response.content


def test_simple_put(tmp_path, available_port):
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

        status, body = put_buffer(port, BUFFER)
        assert status == 200 and body == b"OK", (status, body)

        status, body = bugged_put_truncated_checksum(port, BUFFER)
        assert status == 400, (status, body)
        payload = json.loads(body.decode())
        payload["exception"].pop("url", None)
        assert payload == {
            "message": "Invalid data",
            "exception": {
                "type": "value_error",
                "loc": ["path", "checksum"],
                "msg": "Value error, Wrong length",
                "input": calculate_checksum(BUFFER)[:10],
            },
        }

        status, body = bugged_put_partial_buffer(port, BUFFER)
        assert status == 400 and body == b"Incorrect checksum", (status, body)

        status, body = bugged_put_wrong_param(port, BUFFER)
        assert status == 400 and body == b"Incorrect checksum", (status, body)

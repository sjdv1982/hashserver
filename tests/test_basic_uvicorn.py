import json
import time

import requests

from .utils import start_server, wait_for_server


HELLO_CHECKSUM = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23539"
OTHER_CHECKSUM = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23530"
BAD_CHECKSUM_SHORT = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b"
BAD_CHECKSUM_NON_HEX = "xx25d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23539"


def request_checksum(port: int, checksum: str):
    response = requests.get(f"http://127.0.0.1:{port}/{checksum}", timeout=5)
    status = response.status_code
    try:
        body = response.text
    except Exception:
        body = response.content
    return status, body


def test_basic_uvicorn(bufferdir, available_port):
    port = available_port
    env = {
        "HASHSERVER_DIRECTORY": str(bufferdir),
        "HASHSERVER_LAYOUT": "flat",
    }
    command = [
        "uvicorn",
        "hashserver:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
    ]

    with start_server(command, env=env) as _server:
        wait_for_server(port)

        start = time.monotonic()
        status, body = request_checksum(port, OTHER_CHECKSUM)
        assert status == 404, (status, body)
        assert body == "Not found", body
        assert time.monotonic() - start < 1

        start = time.monotonic()
        status, body = request_checksum(port, HELLO_CHECKSUM)
        assert status == 200, (status, body)
        assert body == "Hello world!\n", body
        assert time.monotonic() - start < 1

        start = time.monotonic()
        status, body = request_checksum(port, BAD_CHECKSUM_SHORT)
        assert status == 400, (status, body)
        assert time.monotonic() - start < 1
        payload = json.loads(body)
        assert isinstance(payload, dict), payload
        assert isinstance(payload.get("exception"), dict), payload
        payload["exception"].pop("url", None)
        assert payload == {
            "message": "Invalid data",
            "exception": {
                "type": "value_error",
                "loc": ["path", "checksum"],
                "msg": "Value error, Wrong length",
                "input": BAD_CHECKSUM_SHORT,
            },
        }

        start = time.monotonic()
        status, body = request_checksum(port, BAD_CHECKSUM_NON_HEX)
        assert status == 400, (status, body)
        assert time.monotonic() - start < 1
        payload = json.loads(body)
        assert isinstance(payload, dict), payload
        assert isinstance(payload.get("exception"), dict), payload
        payload["exception"].pop("url", None)
        assert payload == {
            "message": "Invalid data",
            "exception": {
                "type": "value_error",
                "loc": ["path", "checksum"],
                "msg": "Value error, non-hexadecimal number found in fromhex() arg at position 0",
                "input": BAD_CHECKSUM_NON_HEX,
            },
        }

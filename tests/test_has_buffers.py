import json
import time

import requests

from .utils import start_server, wait_for_server


HELLO_CHECKSUM = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23539"
OTHER_CHECKSUM = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23530"
BAD_CHECKSUM_SHORT = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b"
BAD_CHECKSUM_NON_HEX = "xx25d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23539"


def has_buffers(port: int, checksums):
    response = requests.get(
        f"http://127.0.0.1:{port}/has", json=checksums, timeout=5
    )
    status = response.status_code
    try:
        body = response.json()
    except Exception:
        try:
            body = response.text
        except Exception:
            body = response.content
    return status, body


def test_has_buffers(bufferdir, available_port):
    port = available_port
    command = [
        "hashserver",
        str(bufferdir),
        "--layout",
        "flat",
        "--port",
        str(port),
    ]

    with start_server(command) as _server:
        wait_for_server(port)

        start = time.monotonic()
        status, body = has_buffers(port, [HELLO_CHECKSUM])
        assert status == 200, (status, body)
        assert body == [True], body
        assert time.monotonic() - start < 1

        start = time.monotonic()
        status, body = has_buffers(port, [HELLO_CHECKSUM, OTHER_CHECKSUM])
        assert status == 200, (status, body)
        assert body == [True, False], body
        assert time.monotonic() - start < 1

        start = time.monotonic()
        status, body = has_buffers(
            port, [HELLO_CHECKSUM, OTHER_CHECKSUM, BAD_CHECKSUM_SHORT]
        )
        assert status == 400, (status, body)
        assert time.monotonic() - start < 1
        assert isinstance(body, dict), body
        assert isinstance(body.get("exception"), dict), body
        body["exception"].pop("url", None)
        assert body == {
            "message": "Invalid data",
            "exception": {
                "type": "value_error",
                "loc": ["body", 2],
                "msg": "Value error, Wrong length",
                "input": BAD_CHECKSUM_SHORT,
            },
        }

        start = time.monotonic()
        status, body = has_buffers(
            port, [HELLO_CHECKSUM, OTHER_CHECKSUM, BAD_CHECKSUM_NON_HEX]
        )
        assert status == 400, (status, body)
        assert time.monotonic() - start < 1
        assert isinstance(body, dict), body
        assert isinstance(body.get("exception"), dict), body
        body["exception"].pop("url", None)
        assert body == {
            "message": "Invalid data",
            "exception": {
                "type": "value_error",
                "loc": ["body", 2],
                "msg": "Value error, non-hexadecimal number found in fromhex() arg at position 0",
                "input": BAD_CHECKSUM_NON_HEX,
            },
        }

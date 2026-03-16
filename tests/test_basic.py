import json
import time

import requests

from .utils import start_server, wait_for_server


HELLO_CHECKSUM = "0ba904eae8773b70c75333db4de2f3ac45a8ad4ddba1b242f0b3cfc199391dd8"
OTHER_CHECKSUM = "0ba904eae8773b70c75333db4de2f3ac45a8ad4ddba1b242f0b3cfc199391dd0"
BAD_CHECKSUM_SHORT = "0ba904eae8773b70c75333db4de2f3ac45a8ad4ddba1b242f0"
BAD_CHECKSUM_NON_HEX = "xxa904eae8773b70c75333db4de2f3ac45a8ad4ddba1b242f0b3cfc199391dd8"


def request_checksum(port: int, checksum: str):
    response = requests.get(f"http://127.0.0.1:{port}/{checksum}", timeout=5)
    status = response.status_code
    try:
        body = response.text
    except Exception:
        body = response.content
    return status, body


def test_basic(bufferdir, available_port):
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

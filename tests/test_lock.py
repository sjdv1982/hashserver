import json
import threading
import time
from pathlib import Path

import requests

from .utils import start_server, wait_for_server


def request_checksum(port: int, checksum: str):
    response = requests.get(f"http://127.0.0.1:{port}/{checksum}", timeout=5)
    status = response.status_code
    try:
        body = response.text
    except Exception:
        body = response.content
    return status, body


def test_lock(bufferdir, lorem_text, lorem_checksum, available_port):
    port = available_port
    command = [
        "hashserver",
        str(bufferdir),
        "--lock-timeout",
        "5",
        "--layout",
        "flat",
        "--port",
        str(port),
    ]

    lorem_file = Path(bufferdir) / lorem_checksum
    lorem_lock = Path(str(lorem_file) + ".LOCK")
    if lorem_file.exists():
        lorem_file.unlink()
    if lorem_lock.exists():
        lorem_lock.unlink()

    with start_server(command) as _server:
        wait_for_server(port)

        start = time.monotonic()
        status, body = request_checksum(port, lorem_checksum)
        assert status == 404, (status, body)
        assert body == "Not found", body
        assert time.monotonic() - start < 5

        lorem_lock.touch()

        start = time.monotonic()
        status, body = request_checksum(port, lorem_checksum)
        assert status == 404, (status, body)
        assert body == "Not found", body
        assert time.monotonic() - start > 5

        lorem_file.write_text(lorem_text[:10])

        start = time.monotonic()
        status, body = request_checksum(port, lorem_checksum)
        assert status == 400, (status, body)
        payload = json.loads(body)
        expected_message = (
            "File corruption: file at path {} does not have the correct SHA3-256 checksum.".format(
                lorem_file
            )
        )
        assert payload == {"message": expected_message}

        def finish_lorem():
            remaining = lorem_text[10:]
            with open(lorem_file, "a") as handle:
                while remaining:
                    handle.write(remaining[:50])
                    handle.flush()
                    remaining = remaining[50:]
                    time.sleep(0.5)

        lorem_lock.touch()
        thread = threading.Thread(target=finish_lorem, daemon=True)
        thread.start()
        time.sleep(1)

        start = time.monotonic()
        status, body = request_checksum(port, lorem_checksum)
        assert status == 200, (status, body)
        assert body == lorem_text, body
        assert time.monotonic() - start > 4

        thread.join(timeout=1)

        if lorem_lock.exists():
            lorem_lock.unlink()

    if lorem_file.exists():
        lorem_file.unlink()

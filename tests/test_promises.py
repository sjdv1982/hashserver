import threading
import time
from hashlib import sha3_256

import requests

from .utils import start_server, wait_for_server


def calculate_checksum(buffer: bytes) -> str:
    return sha3_256(buffer).digest().hex()


TEST_BUFFER = b"promise-buffer"
TEST_CHECKSUM = calculate_checksum(TEST_BUFFER)


def _promise(port: int, checksum: str):
    return requests.put(
        f"http://127.0.0.1:{port}/promise/{checksum}",
        timeout=5,
    )


def _has(port: int, checksums):
    response = requests.get(
        f"http://127.0.0.1:{port}/has", json=checksums, timeout=5
    )
    return response.status_code, response.json()


def test_has_reports_promised_checksum(tmp_path, available_port):
    write_dir = tmp_path / "writedir"
    write_dir.mkdir()

    port = available_port
    command = [
        "hashserver",
        str(write_dir),
        "--layout",
        "flat",
        "--port",
        str(port),
    ]

    with start_server(command) as _server:
        wait_for_server(port)

        response = _promise(port, TEST_CHECKSUM)
        assert response.status_code == 202, response.text

        status, body = _has(port, [TEST_CHECKSUM])
        assert status == 200, (status, body)
        assert body == [True]


def test_get_waits_for_promised_upload(tmp_path, available_port):
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

        buffer_checksum = TEST_CHECKSUM
        response = _promise(port, buffer_checksum)
        assert response.status_code == 202, response.text

        results = {}

        def fetch_buffer():
            try:
                resp = requests.get(
                    f"http://127.0.0.1:{port}/{buffer_checksum}",
                    timeout=10,
                )
            except Exception as exc:  # pragma: no cover - debug aid
                results["error"] = exc
            else:
                results["status"] = resp.status_code
                results["body"] = resp.content

        thread = threading.Thread(target=fetch_buffer)
        thread.start()
        time.sleep(0.3)

        upload = requests.put(
            f"http://127.0.0.1:{port}/{buffer_checksum}",
            data=TEST_BUFFER,
            timeout=5,
        )
        assert upload.status_code == 200, upload.text

        thread.join(timeout=10)
        assert not thread.is_alive()
        assert "error" not in results, results.get("error")
        assert "status" in results, results
        assert results["status"] == 200, results
        assert results["body"] == TEST_BUFFER

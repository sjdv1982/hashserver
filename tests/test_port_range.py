import re
import time

import requests

from .utils import allocate_port_range, start_server, wait_for_server


def wait_for_port(process, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    captured = []
    port = None
    while time.monotonic() < deadline:
        line = process.stdout.readline()
        if not line:
            if process.poll() is not None:
                break
            time.sleep(0.1)
            continue
        captured.append(line)
        match = re.search(r"Uvicorn running on http://[^:]+:(\d+)", line)
        if match:
            port = int(match.group(1))
            break
    return port, captured


def test_port_range(bufferdir):
    start, end = allocate_port_range(8)
    command = [
        "hashserver",
        str(bufferdir),
        "--layout",
        "flat",
        "--port-range",
        str(start),
        str(end),
    ]

    with start_server(command, text=True) as process:
        port, captured = wait_for_port(process)
        assert port is not None, "Server did not report the selected port"
        assert start <= port <= end, port

        wait_for_server(port)

        response = requests.get(f"http://127.0.0.1:{port}/healthcheck", timeout=5)
        assert response.status_code == 200, response.text
        assert response.text == "OK"

        if captured:
            print("Initial logs:")
            for line in captured:
                print(line, end="")

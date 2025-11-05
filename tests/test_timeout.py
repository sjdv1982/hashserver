import time

import requests

from .utils import start_server, wait_for_server


def test_timeout_stops_server(bufferdir, available_port):
    port = available_port
    command = [
        "hashserver",
        str(bufferdir),
        "--layout",
        "flat",
        "--port",
        str(port),
        "--timeout",
        "2",
    ]

    with start_server(command) as process:
        wait_for_server(port)

        # Send a request to ensure the timeout resets after activity.
        response = requests.get(f"http://127.0.0.1:{port}/healthcheck", timeout=5)
        assert response.status_code == 200

        # Allow more than the timeout interval without requests; process should exit.
        time.sleep(3)
        exited = process.wait(timeout=5)
        assert exited == 0

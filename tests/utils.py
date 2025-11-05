import contextlib
import os
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import Tuple

import requests


TESTS_DIR = Path(__file__).resolve().parent


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def allocate_port_range(size: int = 5) -> Tuple[int, int]:
    if size <= 0:
        raise ValueError("size must be positive")
    for start in range(40000, 60000):
        sockets = []
        try:
            for offset in range(size):
                candidate = start + offset
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(("127.0.0.1", candidate))
                sockets.append(sock)
            return start, start + size - 1
        except OSError:
            for sock in sockets:
                sock.close()
            continue
        finally:
            for sock in sockets:
                sock.close()
    raise RuntimeError("Unable to allocate a free port range")


def wait_for_server(port: int, path: str = "/healthcheck", timeout: float = 10.0):
    url = f"http://127.0.0.1:{port}{path}"
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(url, timeout=1.0)
        except requests.RequestException as exc:  # pragma: no cover - network error detail only
            last_error = exc
            time.sleep(0.1)
            continue
        if response.status_code == 200:
            return response
        last_error = response.text
        time.sleep(0.1)
    raise RuntimeError(f"Server at {url} did not become ready: {last_error}")


@contextlib.contextmanager
def start_server(command, *, env=None, cwd=None, text: bool = True):
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    process = subprocess.Popen(  # noqa: S603,S607 - controlled command for tests
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=full_env,
        cwd=cwd,
        text=text,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            stdout, _ = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, _ = process.communicate()
        if stdout:
            print("Server logs:")
            print(stdout, end="" if stdout.endswith("\n") else "\n")

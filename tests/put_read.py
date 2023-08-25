# run from the main directory

import subprocess
import signal
import time
import os
from hashlib import sha3_256
import requests

BUFFER = b"""This is a buffer
that is used
for testing purposes"""


def calculate_checksum(buffer: bytes):
    """Return SHA3-256 checksum"""
    return sha3_256(buffer).digest().hex()


def get(checksum):
    response = requests.get(f"http://localhost:8000/{checksum}")
    status = response.status_code
    output = response.content
    return status, output


def put(buffer: bytes):
    assert isinstance(buffer, bytes)
    checksum = calculate_checksum(buffer)
    params = {"buffer": buffer}
    response = requests.put(f"http://localhost:8000/{checksum}", params=params)
    return response.status_code, response.content


os.system("rm -rf tests/writedir")
os.makedirs("tests/writedir")
server = subprocess.Popen(
    "python hashserver.py tests/writedir --w",
    shell=True,
    stderr=subprocess.STDOUT,
    stdout=subprocess.PIPE,
    executable="/bin/bash",
)
time.sleep(1)

try:
    t = time.time()
    status, output = put(BUFFER)
    assert status == 200, (status, output)
    assert output == b"OK", output
    assert time.time() - t < 1, time.time() - t

    checksum = calculate_checksum(BUFFER)

    t = time.time()
    status, output = get(checksum)
    assert status == 200, (status, output)
    assert output == BUFFER, output
    assert time.time() - t < 1, time.time() - t

    print("END")
finally:
    os.system("rm -rf tests/writedir")

    print("Server logs:")
    server.send_signal(signal.SIGINT)
    output, _ = server.communicate(timeout=5)
    try:
        print(output.decode())
    except Exception:
        print(output)

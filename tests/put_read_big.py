import subprocess
import signal
import time
import os
from hashlib import sha3_256
import requests
import random

random.seed(0)
BUFFER = b""
for n in range(10):
    BUFFER += random.randbytes(10**8)


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
    response = requests.put(f"http://localhost:8000/{checksum}", data=buffer)
    return response.status_code, response.content


os.system("rm -rf writedir")
os.makedirs("writedir")
os.environ["HASHSERVER_DIRECTORY"] = "writedir"
os.environ["HASHSERVER_WRITABLE"] = "1"
server = subprocess.Popen(
    "uvicorn hashserver:app --log-level warning",
    shell=True,
    stderr=subprocess.STDOUT,
    stdout=subprocess.PIPE,
    executable="/bin/bash",
)
time.sleep(1)

print("Start")
t = time.time()
try:
    print("Put")
    status, output = put(BUFFER)
    assert status == 200, (status, output)
    assert output == b"OK", output
    print("...done after {:.1f} seconds".format(time.time() - t))

    print("Calculate checksum")
    checksum = calculate_checksum(BUFFER)
    print("...done after {:.1f} seconds".format(time.time() - t))

    print("Read")
    status, output = get(checksum)
    assert status == 200, (status, output)
    assert output == BUFFER, output
    print("...done after {:.1f} seconds".format(time.time() - t))

    print("END")
finally:
    os.system("rm -rf writedir")

    print("Server logs:")
    server.send_signal(signal.SIGINT)
    output, _ = server.communicate(timeout=5)
    try:
        print(output.decode())
    except Exception:
        print(output)

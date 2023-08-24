# run from the main directory

import subprocess
import signal
import time
import os
import json
import requests

checksum1 = "31dfbc040ae6ff1f7ac1ac4d759d50be1be9a1c5de0a8147e2c48eea4e45eb87"
checksum2 = "1c325e4308724a7b7f7a1ad9fa0c9b7a20e04f8028b755110083e2b5a45ab92a"
checksum3 = "b04bf4dde649476037dc8311cd58c5ff0cff52404582c38c3a70b71245dc2ece"
checksum4 = "cf92e4229286e67bf56378a5eb383edf13ce5e7a507fa5cf08aa5c2da4e35572"
other_checksum = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23530"


def request(checksum):
    response = requests.get(f"http://localhost:8000/{checksum}")
    status = response.status_code
    try:
        output = response.text
    except Exception:
        output = response.content
    return status, output


server = subprocess.Popen(
    "python hashserver.py tests/vaultdir",
    shell=True,
    stderr=subprocess.STDOUT,
    stdout=subprocess.PIPE,
    executable="/bin/bash",
)
time.sleep(1)

try:

    t = time.time()
    status, output = request(checksum1)
    assert status == 200, (status, output)
    assert output == '"buffer1"\n', output
    assert time.time() - t < 1, time.time() - t

    t = time.time()
    status, output = request(checksum2)
    assert status == 200, (status, output)
    assert output == '"buffer2"\n', output
    assert time.time() - t < 1, time.time() - t

    t = time.time()
    status, output = request(checksum3)
    assert status == 200, (status, output)
    assert output == '"buffer3"\n', output
    assert time.time() - t < 1, time.time() - t

    t = time.time()
    status, output = request(checksum4)
    assert status == 200, (status, output)
    assert output == '"buffer4"\n', output
    assert time.time() - t < 1, time.time() - t

    t = time.time()
    status, output = request(other_checksum)
    assert status == 404, status
    assert output == "Not found", output
    assert time.time() - t < 1, time.time() - t

    print("END")
finally:
    print("Server logs:")
    server.send_signal(signal.SIGINT)
    output, _ = server.communicate(timeout=5)
    try:
        print(output.decode())
    except Exception:
        print(output)

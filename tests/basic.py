import subprocess
import signal
import time
import os
import json
import requests

hello_checksum = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23539"
other_checksum = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23530"
bad_checksum1 = "6825d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b"
bad_checksum2 = "xx25d69119b014b3d5ac9b17ce68bf98b66190c5e34397781b3776dca9c23539"


def request(checksum):
    response = requests.get(f"http://localhost:8000/{checksum}")
    status = response.status_code
    try:
        output = response.text
    except Exception:
        output = response.content
    return status, output


server = subprocess.Popen(
    "hashserver bufferdir --layout flat",
    shell=True,
    stderr=subprocess.STDOUT,
    stdout=subprocess.PIPE,
    executable="/bin/bash",
)
time.sleep(1)

try:
    t = time.time()
    status, output = request(other_checksum)
    assert status == 404, (status, output)
    assert output == "Not found", output
    assert time.time() - t < 1, time.time() - t

    t = time.time()
    status, output = request(hello_checksum)
    assert status == 200, (status, output)
    assert output == "Hello world!\n", output
    assert time.time() - t < 1, time.time() - t

    t = time.time()
    status, output = request(bad_checksum1)
    assert status == 400, (status, output)
    assert time.time() - t < 1, time.time() - t
    try:
        output = json.loads(output)
    except Exception:
        raise AssertionError(output) from None
    assert isinstance(output, dict), output
    assert isinstance(output.get("exception"), dict), output
    output["exception"].pop("url", None)
    refe_output = {
        "message": "Invalid data",
        "exception": {
            "type": "value_error",
            "loc": ["path", "checksum"],
            "msg": "Value error, Wrong length",
            "input": bad_checksum1,
        },
    }
    assert output == refe_output, output

    t = time.time()
    status, output = request(bad_checksum2)
    assert status == 400, (status, output)
    assert time.time() - t < 1, time.time() - t
    try:
        output = json.loads(output)
    except Exception:
        raise AssertionError(output) from None
    assert isinstance(output, dict), output
    assert isinstance(output.get("exception"), dict), output
    output["exception"].pop("url", None)
    refe_output = {
        "message": "Invalid data",
        "exception": {
            "type": "value_error",
            "loc": ["path", "checksum"],
            "msg": "Value error, non-hexadecimal number found in fromhex() arg at position 0",
            "input": bad_checksum2,
        },
    }
    assert output == refe_output, output

    print("END")
finally:
    print("Server logs:")
    server.send_signal(signal.SIGINT)
    output, _ = server.communicate(timeout=5)
    try:
        print(output.decode())
    except Exception:
        print(output)

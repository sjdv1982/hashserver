# run from the main directory

import subprocess
import signal
import time
import os
import pathlib
import json
import threading
import requests

DIRECTORY = "tests/bufferdir"
with open("tests/lorem_ipsum.txt") as f:
    lorem = f.read()
lorem_checksum = "bde3f269175e1dcda13848278aa6046bd643cea85b84c8b8bb80952e70b6eae0"
lorem_file = os.path.join(DIRECTORY, lorem_checksum)
global_lock = os.path.join(DIRECTORY, ".LOCK")

try:
    os.unlink(lorem_file)
except FileNotFoundError:
    pass

try:
    os.unlink(global_lock)
except FileNotFoundError:
    pass


def request(checksum):
    response = requests.get(f"http://localhost:8000/{checksum}")
    status = response.status_code
    try:
        output = response.text
    except Exception:
        output = response.content
    return status, output


server = subprocess.Popen(
    "python hashserver.py tests/bufferdir --lock-timeout 5",
    shell=True,
    stderr=subprocess.STDOUT,
    stdout=subprocess.PIPE,
    executable="/bin/bash",
)
time.sleep(1)

try:
    t = time.time()
    status, output = request(lorem_checksum)
    assert status == 404, (status, output)
    assert output == "Not found", output
    assert time.time() - t < 5

    global_lockp = pathlib.Path(global_lock)
    global_lockp.touch()

    t = time.time()
    status, output = request(lorem_checksum)
    assert status == 404, (status, output)
    assert output == "Not found", output
    assert time.time() - t > 5, time.time() - t

    with open(lorem_file, "w") as f:
        f.write(lorem[:10])

    t = time.time()
    status, output = request(lorem_checksum)
    assert status == 400, (status, output)
    try:
        output = json.loads(output)
    except Exception:
        raise AssertionError(output) from None
    refe_output = {
        "message": "File corruption: file at path tests/bufferdir/bde3f269175e1dcda13848278aa6046bd643cea85b84c8b8bb80952e70b6eae0 does not have the correct SHA3-256 checksum."
    }
    assert output == refe_output, output

    def complete_lorem():
        d = lorem[10:]
        with open(lorem_file, "a") as f:
            while len(d):
                f.write(d[:50])
                f.flush()
                d = d[50:]
                time.sleep(0.5)

    global_lockp.touch()
    thread = threading.Thread(target=complete_lorem, daemon=True)
    thread.start()
    time.sleep(1)

    t = time.time()
    status, output = request(lorem_checksum)
    assert status == 200, (status, output)
    assert output == lorem, output
    assert time.time() - t > 4, time.time() - t

    global_lockp.unlink()

    print("END")
finally:
    print("Server logs:")
    server.send_signal(signal.SIGINT)
    output, _ = server.communicate(timeout=5)
    try:
        print(output.decode())
    except Exception:
        print(output)

try:
    os.unlink(lorem_file)
except FileNotFoundError:
    pass

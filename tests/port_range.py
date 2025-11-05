import subprocess
import signal
import time
import re
import requests

COMMAND = "hashserver bufferdir --layout flat --port-range 49300 49310"


def wait_for_port(process, timeout=10):
    deadline = time.time() + timeout
    captured = []
    port = None
    while time.time() < deadline:
        line = process.stdout.readline()
        if not line:
            if process.poll() is not None:
                break
            time.sleep(0.1)
            continue
        captured.append(line)
        if "Uvicorn running on" in line:
            match = re.search(r"http://[^:]+:(\d+)", line)
            if match:
                port = int(match.group(1))
                break
    return port, captured


server = subprocess.Popen(
    COMMAND,
    shell=True,
    stderr=subprocess.STDOUT,
    stdout=subprocess.PIPE,
    executable="/bin/bash",
    text=True,
)

initial_logs = []

try:
    port, initial_logs = wait_for_port(server)
    assert port is not None, "Server did not report the selected port"
    assert 49300 <= port <= 49310, port

    response = requests.get(f"http://127.0.0.1:{port}/healthcheck")
    assert response.status_code == 200, response.text
    assert response.text == "OK"
finally:
    server.send_signal(signal.SIGINT)
    try:
        output, _ = server.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        server.kill()
        output, _ = server.communicate()
    print("Server logs:")
    for line in initial_logs:
        print(line, end="")
    if output:
        print(output, end="" if output.endswith("\n") else "\n")

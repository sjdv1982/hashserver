# requires a running hashserver, URL supplied as first argument

import requests
from hashlib import sha3_256
import sys
import json

hashserver_url = sys.argv[1]


def calculate_checksum(buffer: bytes):
    """Return SHA3-256 checksum"""
    return sha3_256(buffer).digest().hex()


def put(buffer: bytes):
    assert isinstance(buffer, bytes)
    checksum = calculate_checksum(buffer)
    url = hashserver_url + "/" + checksum
    params = {"buffer": buffer}
    response = requests.put(url, params=params)
    return response.status_code, response.content


def bugged_put1(buffer: bytes):
    assert isinstance(buffer, bytes)
    checksum = calculate_checksum(buffer)
    url = hashserver_url + "/" + checksum[:10]
    params = {"buffer": buffer}
    response = requests.put(url, params=params)
    return response.status_code, response.content


def bugged_put2(buffer: bytes):
    assert isinstance(buffer, bytes)
    checksum = calculate_checksum(buffer)
    url = hashserver_url + "/" + checksum
    params = {"buffer": buffer[:10]}
    response = requests.put(url, params=params)
    return response.status_code, response.content


def bugged_put3(buffer: bytes):
    assert isinstance(buffer, bytes)
    checksum = calculate_checksum(buffer)
    url = hashserver_url + "/" + checksum
    params = {"buf": buffer}
    response = requests.put(url, params=params)
    return response.status_code, response.content


buffer1 = b"This is a test"
status_code, response = put(buffer1)
assert status_code == 200 and response == b"OK", (status_code, response)

status_code, response = bugged_put1(buffer1)
assert status_code == 400, (status_code, response)
try:
    response = json.loads(response.decode())
    response["exception"].pop("url")
except Exception:
    raise AssertionError((status_code, response))
refe_response = {
    "message": "Invalid data",
    "exception": {
        "type": "value_error",
        "loc": ["path", "checksum"],
        "msg": "Value error, Wrong length",
        "input": "3c3b66edcf",
    },
}
assert response == refe_response, response

status_code, response = bugged_put2(buffer1)
assert status_code == 400 and response == b"Incorrect checksum", (status_code, response)

status_code, response = bugged_put3(buffer1)
assert status_code == 400, (status_code, response)
try:
    response = json.loads(response.decode())
    response["exception"].pop("url")
except Exception:
    raise AssertionError((status_code, response))
refe_response = {
    "message": "Invalid data",
    "exception": {
        "type": "missing",
        "loc": ["query", "buffer"],
        "msg": "Field required",
        "input": None,
    },
}
assert response == refe_response, response

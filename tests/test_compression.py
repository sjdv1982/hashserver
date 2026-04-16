import gzip
from hashlib import sha256
import sys

import requests
import zstandard

from .utils import start_server, wait_for_server


BUFFER = (b"compressed payload\n" * 128) + b"tail"


def calculate_checksum(buffer: bytes) -> str:
    return sha256(buffer).digest().hex()


def compress_zstd(buffer: bytes) -> bytes:
    return zstandard.ZstdCompressor().compress(buffer)


def put_buffer(port: int, checksum: str, buffer: bytes, *, content_encoding=None):
    headers = {}
    if content_encoding is not None:
        headers["Content-Encoding"] = content_encoding
    response = requests.put(
        f"http://127.0.0.1:{port}/{checksum}",
        data=buffer,
        headers=headers,
        timeout=10,
    )
    return response


def get_buffer_raw(port: int, checksum: str):
    response = requests.get(
        f"http://127.0.0.1:{port}/{checksum}",
        stream=True,
        timeout=10,
    )
    response.raw.decode_content = False
    body = response.raw.read()
    return response, body


def has_buffers(port: int, checksums):
    response = requests.get(f"http://127.0.0.1:{port}/has", json=checksums, timeout=10)
    return response


def buffer_lengths(port: int, checksums):
    response = requests.get(
        f"http://127.0.0.1:{port}/buffer-length", json=checksums, timeout=10
    )
    return response


def _server_command(write_dir, port):
    env = {
        "HASHSERVER_DIRECTORY": str(write_dir),
        "HASHSERVER_PORT": str(port),
        "HASHSERVER_WRITABLE": "1",
        "HASHSERVER_LAYOUT": "flat",
    }
    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "hashserver:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-level",
        "warning",
    ]
    return command, env


def _readonly_server_command(read_dir, port):
    env = {
        "HASHSERVER_DIRECTORY": str(read_dir),
        "HASHSERVER_PORT": str(port),
        "HASHSERVER_LAYOUT": "flat",
    }
    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "hashserver:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-level",
        "warning",
    ]
    return command, env


def test_put_zstd_stores_compressed_and_sidecar(tmp_path, available_port):
    write_dir = tmp_path / "writedir"
    write_dir.mkdir()
    checksum = calculate_checksum(BUFFER)
    compressed = compress_zstd(BUFFER)

    command, env = _server_command(write_dir, available_port)

    with start_server(command, env=env) as _server:
        wait_for_server(available_port)

        response = put_buffer(
            available_port,
            checksum,
            compressed,
            content_encoding="zstd",
        )
        assert response.status_code == 200, response.content

        stored = write_dir / f"{checksum}.zst"
        assert stored.read_bytes() == compressed
        assert (write_dir / f"{checksum}.BUFFERLENGTH").read_text() == str(len(BUFFER))


def test_put_compressed_checksum_mismatch_returns_400(tmp_path, available_port):
    write_dir = tmp_path / "writedir"
    write_dir.mkdir()
    checksum = calculate_checksum(BUFFER)
    compressed = compress_zstd(BUFFER + b"x")

    command, env = _server_command(write_dir, available_port)

    with start_server(command, env=env) as _server:
        wait_for_server(available_port)

        response = put_buffer(
            available_port,
            checksum,
            compressed,
            content_encoding="zstd",
        )
        assert response.status_code == 400, response.content
        assert not (write_dir / f"{checksum}.zst").exists()
        assert not (write_dir / f"{checksum}.BUFFERLENGTH").exists()


def test_put_compressed_and_uncompressed_can_coexist(tmp_path, available_port):
    write_dir = tmp_path / "writedir"
    write_dir.mkdir()
    checksum = calculate_checksum(BUFFER)
    compressed = compress_zstd(BUFFER)

    command, env = _server_command(write_dir, available_port)

    with start_server(command, env=env) as _server:
        wait_for_server(available_port)

        response = put_buffer(
            available_port,
            checksum,
            compressed,
            content_encoding="zstd",
        )
        assert response.status_code == 200, response.content

        response = put_buffer(available_port, checksum, BUFFER)
        assert response.status_code == 200, response.content

        assert (write_dir / checksum).read_bytes() == BUFFER
        assert (write_dir / f"{checksum}.zst").read_bytes() == compressed
        assert not (write_dir / f"{checksum}.BUFFERLENGTH").exists()


def test_get_prefers_compressed_when_only_compressed_exists(tmp_path, available_port):
    read_dir = tmp_path / "bufferdir"
    read_dir.mkdir()
    checksum = calculate_checksum(BUFFER)
    compressed = compress_zstd(BUFFER)
    (read_dir / f"{checksum}.zst").write_bytes(compressed)
    (read_dir / f"{checksum}.BUFFERLENGTH").write_text(str(len(BUFFER)))

    command, env = _readonly_server_command(read_dir, available_port)

    with start_server(command, env=env) as _server:
        wait_for_server(available_port)

        response, body = get_buffer_raw(available_port, checksum)
        assert response.status_code == 200, body
        assert response.headers["Content-Encoding"] == "zstd"
        assert body == compressed


def test_get_prefers_uncompressed_when_both_forms_exist(tmp_path, available_port):
    read_dir = tmp_path / "bufferdir"
    read_dir.mkdir()
    checksum = calculate_checksum(BUFFER)
    compressed = gzip.compress(BUFFER)
    (read_dir / checksum).write_bytes(BUFFER)
    (read_dir / f"{checksum}.gz").write_bytes(compressed)

    command, env = _readonly_server_command(read_dir, available_port)

    with start_server(command, env=env) as _server:
        wait_for_server(available_port)

        response, body = get_buffer_raw(available_port, checksum)
        assert response.status_code == 200, body
        assert "Content-Encoding" not in response.headers
        assert body == BUFFER


def test_has_and_buffer_length_recognize_compressed_form(tmp_path, available_port):
    read_dir = tmp_path / "bufferdir"
    read_dir.mkdir()
    checksum = calculate_checksum(BUFFER)
    compressed = compress_zstd(BUFFER)
    (read_dir / f"{checksum}.zst").write_bytes(compressed)
    (read_dir / f"{checksum}.BUFFERLENGTH").write_text(str(len(BUFFER)))

    command, env = _readonly_server_command(read_dir, available_port)

    with start_server(command, env=env) as _server:
        wait_for_server(available_port)

        has_response = has_buffers(available_port, [checksum, "0" * 64])
        assert has_response.status_code == 200, has_response.text
        assert has_response.json() == [True, False]

        length_response = buffer_lengths(available_port, [checksum])
        assert length_response.status_code == 200, length_response.text
        assert length_response.json() == [len(BUFFER)]

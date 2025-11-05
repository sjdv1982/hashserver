import os
import sys
import argparse
import random
import socket
from typing import Union, List
from hashlib import sha3_256

from fastapi import FastAPI, Path, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder

from functools import partial

from hash_file_response import parse_checksum, HashFileResponse, PrefixHashFileResponse

from typing_extensions import Annotated
from pydantic.functional_validators import BeforeValidator

import anyio
import pathlib
import time

Checksum = Annotated[
    Union[str, bytes], BeforeValidator(partial(parse_checksum, as_bytes=False))
]


def calculate_checksum(buffer):
    """Return SHA3-256 checksum"""
    return sha3_256(buffer).digest().hex()


def calculate_checksum_stream():
    return sha3_256()


def pick_random_free_port(host: str, start: int, end: int) -> int:
    if start < 0 or end > 65535:
        raise RuntimeError("--port-range values must be between 0 and 65535")
    if start > end:
        raise RuntimeError("--port-range START must be less than or equal to END")

    span = end - start + 1
    attempted = set()
    while len(attempted) < span:
        port = random.randint(start, end)
        if port in attempted:
            continue
        attempted.add(port)
        try:
            with socket.create_server((host, port), reuse_port=False):
                pass
        except OSError:
            continue
        return port

    raise RuntimeError(f"No free port available in range {start}-{end}")


DEFAULT_LOCK_TIMEOUT = 120.0
CHUNK_SIZE = 640 * 1024  # for now, hardcoded

env = os.environ
as_commandline_tool = True

if "HASHSERVER_DIRECTORY" in os.environ:
    directory = os.environ["HASHSERVER_DIRECTORY"]
    lock_timeout = DEFAULT_LOCK_TIMEOUT
    if "HASHSERVER_LOCK_TIMEOUT" in os.environ:
        lock_timeout = float(os.environ["HASHSERVER_LOCK_TIMEOUT"])
    writable = False
    if "HASHSERVER_WRITABLE" in os.environ:
        env_writable = os.environ["HASHSERVER_WRITABLE"]
        assert env_writable.lower() in ("true", "false", "0", "1", ""), env_writable
        if env_writable.lower() in ("true", "1"):
            writable = True
    as_commandline_tool = False

    extra_dirs = os.environ.get("HASHSERVER_EXTRA_DIRS")
    if extra_dirs:

        def _filt(d):
            d = d.strip()
            if d == '""' or d == "''":
                return ""

        extra_dirs = [_filt(d) for d in extra_dirs.split(";")]
        extra_dirs = [d for d in extra_dirs if d]
    else:
        extra_dirs = []

    layout = os.environ.get("HASHSERVER_LAYOUT", "prefix")

else:
    if (
        len(sys.argv)
        and sys.argv[0].find("uvicorn") > -1
        and not os.path.isdir(sys.argv[0])
    ):
        print(
            "Running hashserver under uvicorn CLI requires at least HASHSERVER_DIRECTORY to be defined",
            file=sys.stderr,
        )
        exit(1)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "directory",
        help="""Directory where buffers are located.

    Buffers have the same file name as their (SHA3-256) checksum.""",
    )
    parser.add_argument(
        "--extra-dirs",
        help="""Extra directories where read-only buffers are located.

This must be a list of directories separated by semi-colons (;).
If not specified, this argument is read from HASHSERVER_EXTRA_DIRS, if present""",
    )

    parser.add_argument(
        "--lock-timeout",
        type=float,
        help="Time-out after which lock files are broken",
        default=DEFAULT_LOCK_TIMEOUT,
    )

    parser.add_argument(
        "--writable",
        action="store_true",
        help="Allow HTTP PUT requests",
    )

    port_group = parser.add_mutually_exclusive_group()
    port_group.add_argument(
        "--port",
        type=int,
        help="Network port",
    )
    port_group.add_argument(
        "--port-range",
        type=int,
        nargs=2,
        metavar=("START", "END"),
        help="Inclusive port range to select a random free port from",
    )

    parser.add_argument(
        "--host",
        type=str,
        help="Network host",
        default="127.0.0.1",
    )

    parser.add_argument(
        "--layout",
        type=str,
        help="""Directory layout.
        One of:
        - "flat". 
        A buffer with checksum CS is stored as file "$DIRECTORY/$CS".

        - "prefix". 
        A buffer with checksum CS is stored as file "$DIRECTORY/$PREFIX/$CS",
        where PREFIX is the first two characters of CS.
                
        """,
        default="prefix",
    )

    args = parser.parse_args()
    directory = args.directory
    lock_timeout = args.lock_timeout
    writable = args.writable
    extra_dirs = args.extra_dirs
    if not extra_dirs:
        extra_dirs = os.environ.get("HASHSERVER_EXTRA_DIRS")
    if extra_dirs:
        extra_dirs = [d.strip() for d in extra_dirs.split(";")]
    else:
        extra_dirs = []
    layout = args.layout
    if args.port_range:
        start, end = args.port_range
        selected_port = pick_random_free_port(args.host, start, end)
    else:
        selected_port = args.port if args.port is not None else 8000
    args.port = selected_port


if not os.path.exists(directory):
    raise FileExistsError(f"Directory '{directory}' does not exist")
if not os.path.isdir(directory):
    raise FileExistsError(f"Directory '{directory}' is not a directory")
if lock_timeout <= 0:
    raise RuntimeError("Lock timeout must be positive")

if layout not in ("flat", "prefix"):
    raise RuntimeError("Layout must be 'flat' or 'prefix'")

app = FastAPI()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    inner_exc = exc.args[0][0]
    inner_exc = jsonable_encoder(inner_exc)
    inner_exc.pop("ctx", None)
    return JSONResponse(
        status_code=400,
        content={"message": "Invalid data", "exception": inner_exc},
    )


@app.exception_handler(FileNotFoundError)
async def filenotfound_exception_handler(request, exc):
    return Response(status_code=404, content="Not found")


@app.exception_handler(RuntimeError)
async def runtime_exception_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"message": f"{exc}"},
    )


async def until_no_lock(lockpath):
    while 1:
        try:
            lock_stat_result = await anyio.to_thread.run_sync(os.stat, lockpath)
        except FileNotFoundError:
            break
        lock_mtime = lock_stat_result.st_mtime
        if time.time() - lock_mtime > lock_timeout:
            break
        await anyio.sleep(1)


@app.get("/has")
async def has_buffers(checksums: Annotated[List[Checksum], Body()]) -> JSONResponse:
    global_lockpath = os.path.join(directory, ".LOCK")
    await until_no_lock(global_lockpath)

    result = []

    for checksum in checksums:
        ok = False
        if layout == "prefix":
            prefix = parse_checksum(checksum)[:2]
            path = os.path.join(directory, prefix, checksum)
        else:
            path = os.path.join(directory, checksum)
        if await anyio.Path(path).exists():
            ok = True
        if ok:
            result.append(True)
        else:
            for extra_dir in extra_dirs:
                path = os.path.join(extra_dir, checksum)
                if await anyio.Path(path).exists():
                    result.append(True)
                    break
            else:
                result.append(False)

    return result


_response_classes_get_file = {
    "flat": HashFileResponse,
    "prefix": PrefixHashFileResponse,
}


@app.get("/healthcheck")
async def healthcheck() -> Response:
    return Response(content="OK")


@app.get("/{checksum}")
async def get_file(checksum: Annotated[Checksum, Path()]) -> HashFileResponse:
    ResponseClass = _response_classes_get_file[layout]
    response = ResponseClass(
        directory=directory, checksum=checksum, extra_dirs=extra_dirs
    )
    response.lock_timeout = lock_timeout
    return response


async def put_file(checksum: Annotated[Checksum, Path()], rq: Request) -> Response:

    cs_stream = calculate_checksum_stream()

    if layout == "prefix":
        prefix = parse_checksum(checksum)[:2]
        path = os.path.join(directory, prefix, checksum)
        global_lockpath = os.path.join(directory, prefix, ".LOCK")

    else:
        path = os.path.join(directory, checksum)
        global_lockpath = os.path.join(directory, ".LOCK")

    if layout == "prefix":
        target_directory = anyio.Path(os.path.join(directory, prefix))
        if not await target_directory.exists():
            await target_directory.mkdir()

    # Acquire the locks, wait for other applications
    # There is a global lock and a file-specific lock to acquire.
    # Currently:
    # - hashserver itself writes a file-specific lock
    # - Nothing writes a global lock for flat or prefix directories
    file_lockpath = path + ".LOCK"
    for lockpath in (global_lockpath, file_lockpath):
        await until_no_lock(lockpath)
    for lockpath in (global_lockpath, file_lockpath):
        try:
            pathlib.Path(lockpath).unlink()
        except FileNotFoundError:
            pass

    # Write (=hold) the file-specific lock, and write the buffer to file
    pathlib.Path(file_lockpath).touch()
    lock_touch_time = time.time()
    ok = False
    try:
        async with await anyio.open_file(path, mode="wb") as file:
            async for chunk in rq.stream():
                cs_stream.update(chunk)
                if time.time() - lock_touch_time > 10:
                    pathlib.Path(file_lockpath).touch()
                    lock_touch_time = time.time()
                await file.write(chunk)
            buffer_checksum = cs_stream.hexdigest()
            if buffer_checksum != checksum:
                return Response(status_code=400, content="Incorrect checksum")
            ok = True
    finally:
        if not ok:
            try:
                pathlib.Path(path).unlink()
            except FileNotFoundError:
                pass
        try:
            pathlib.Path(file_lockpath).unlink()
        except FileNotFoundError:
            pass

    return Response(content="OK")


if writable:
    put_file = app.put("/{checksum}")(put_file)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def main():
    """Console-script shim; server launch happens during module import."""
    return 0


if as_commandline_tool:
    import uvicorn

    uvicorn.run(app, port=args.port, host=args.host)
else:
    # uvicorn (or some other ASGI launcher) will take care of it
    pass

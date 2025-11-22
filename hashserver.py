import os
import sys
import argparse
import random
import socket
import json
import asyncio
import aiofiles
import aiofiles.os
import aiofiles.tempfile
import contextlib
from typing import Union, List
from fastapi import FastAPI, Path, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder
from starlette.requests import ClientDisconnect

from functools import partial

from hash_file_response import (
    parse_checksum,
    HashFileResponse,
    PrefixHashFileResponse,
    CHECKSUM_ALGORITHMS,
    DEFAULT_CHECKSUM_ALGORITHM,
    set_checksum_encoding,
)

from typing_extensions import Annotated
from pydantic.functional_validators import BeforeValidator

import anyio
import pathlib
import time

Checksum = Annotated[
    Union[str, bytes], BeforeValidator(partial(parse_checksum, as_bytes=False))
]

checksum_constructor = CHECKSUM_ALGORITHMS[DEFAULT_CHECKSUM_ALGORITHM]

STATUS_FILE_WAIT_TIMEOUT = 20.0
INACTIVITY_CHECK_INTERVAL = 1.0


INACTIVITY_STATE = {
    "timeout": None,
    "last_request": None,
    "task": None,
    "server": None,
}


def calculate_checksum(buffer):
    """Return checksum in the configured encoding."""
    return checksum_constructor(buffer).digest().hex()


def calculate_checksum_stream():
    return checksum_constructor()


def wait_for_status_file(path: str, timeout: float = STATUS_FILE_WAIT_TIMEOUT):
    deadline = time.monotonic() + timeout
    while True:
        try:
            with open(path, "r", encoding="utf-8") as status_stream:
                contents = json.load(status_stream)
                break
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                print(
                    f"Status file '{path}' not found after {int(timeout)} seconds",
                    file=sys.stderr,
                )
                sys.exit(1)
            time.sleep(0.1)
            continue
        except json.JSONDecodeError as exc:
            print(
                f"Status file '{path}' is not valid JSON: {exc}",
                file=sys.stderr,
            )
            sys.exit(1)

    if not isinstance(contents, dict):
        print(
            f"Status file '{path}' must contain a JSON object",
            file=sys.stderr,
        )
        sys.exit(1)

    return contents


class StatusFileTracker:
    def __init__(self, path: str, base_contents: dict, port: int):
        self.path = path
        self._base_contents = dict(base_contents)
        self.port = port
        self.running_written = False

    def _write(self, payload: dict):
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as status_stream:
            json.dump(payload, status_stream)
            status_stream.write("\n")
        os.replace(tmp_path, self.path)

    def write_running(self):
        payload = dict(self._base_contents)
        payload["port"] = self.port
        payload["status"] = "running"
        self._write(payload)
        self._base_contents = payload
        self.running_written = True

    def write_failed(self):
        payload = dict(self._base_contents)
        payload["status"] = "failed"
        self._write(payload)


def raise_startup_error(exc: BaseException):
    if status_tracker and not status_tracker.running_written:
        status_tracker.write_failed()
    raise exc


def configure_checksum_encoding(encoding: str):
    global checksum_constructor
    try:
        checksum_constructor = CHECKSUM_ALGORITHMS[encoding]
    except KeyError:
        raise_startup_error(
            RuntimeError(
                f"--encoding must be one of: {', '.join(CHECKSUM_ALGORITHMS.keys())}"
            )
        )
    set_checksum_encoding(encoding)


def setup_inactivity_timeout(timeout_seconds: float, server):
    INACTIVITY_STATE["timeout"] = timeout_seconds
    INACTIVITY_STATE["server"] = server

    async def monitor_inactivity():
        try:
            while True:
                await asyncio.sleep(INACTIVITY_CHECK_INTERVAL)
                last_request = INACTIVITY_STATE.get("last_request")
                if last_request is None:
                    continue
                if time.monotonic() - last_request >= timeout_seconds:
                    server.should_exit = True
                    break
        except asyncio.CancelledError:
            raise

    async def start_monitor():
        INACTIVITY_STATE["last_request"] = time.monotonic()
        loop = asyncio.get_running_loop()
        INACTIVITY_STATE["task"] = loop.create_task(monitor_inactivity())

    async def stop_monitor():
        task = INACTIVITY_STATE.get("task")
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            INACTIVITY_STATE["task"] = None
        INACTIVITY_STATE["last_request"] = None
        INACTIVITY_STATE["server"] = None
        INACTIVITY_STATE["timeout"] = None

    app.add_event_handler("startup", start_monitor)
    app.add_event_handler("shutdown", stop_monitor)


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
status_tracker = None
status_file_path = None
status_file_contents = None
timeout_seconds = None

if "HASHSERVER_DIRECTORY" in os.environ:
    directory = os.environ["HASHSERVER_DIRECTORY"]
    writable = False
    if "HASHSERVER_WRITABLE" in os.environ:
        env_writable = os.environ["HASHSERVER_WRITABLE"]
        assert env_writable.lower() in ("true", "false", "0", "1", ""), env_writable
        if env_writable.lower() in ("true", "1"):
            writable = True
    as_commandline_tool = False

    extra_dirs: list[str] = []
    extra_dirs0 = os.environ.get("HASHSERVER_EXTRA_DIRS")
    if extra_dirs0:

        def _filt(d):
            d = d.strip()
            if d == '""' or d == "''":
                return ""

        extra_dirs00 = [_filt(d) for d in extra_dirs0.split(";")]
        extra_dirs = [d for d in extra_dirs00 if d]

    layout = os.environ.get("HASHSERVER_LAYOUT", "prefix")
    status_file_path = None
    status_file_contents = None
    timeout_seconds = None
    encoding = os.environ.get("HASHSERVER_ENCODING", DEFAULT_CHECKSUM_ALGORITHM)
    configure_checksum_encoding(encoding)

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

    Buffers have the same file name as their checksum (sha3-256 by default).""",
    )
    parser.add_argument(
        "--extra-dirs",
        help="""Extra directories where read-only buffers are located.

This must be a list of directories separated by semi-colons (;).
If not specified, this argument is read from HASHSERVER_EXTRA_DIRS, if present""",
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

    parser.add_argument(
        "--encoding",
        type=str,
        choices=tuple(CHECKSUM_ALGORITHMS.keys()),
        default=DEFAULT_CHECKSUM_ALGORITHM,
        help="Hash algorithm used for checksum calculations (default: %(default)s)",
    )

    parser.add_argument(
        "--status-file",
        type=str,
        help="JSON file used to report server status",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        help="Stop the server after this many seconds of inactivity",
    )

    args = parser.parse_args()
    directory = args.directory
    writable = args.writable
    extra_dirs = args.extra_dirs
    configure_checksum_encoding(args.encoding)
    status_file_path = args.status_file
    timeout_seconds = args.timeout
    if status_file_path:
        status_file_contents = wait_for_status_file(status_file_path)
        status_tracker = StatusFileTracker(
            status_file_path, status_file_contents, args.port
        )
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise_startup_error(RuntimeError("--timeout must be a positive number"))
    if not extra_dirs:
        extra_dirs = os.environ.get("HASHSERVER_EXTRA_DIRS")
    if extra_dirs:
        extra_dirs = [d.strip() for d in extra_dirs.split(";")]
    else:
        extra_dirs = []
    layout = args.layout
    if args.port_range:
        start, end = args.port_range
        try:
            selected_port = pick_random_free_port(args.host, start, end)
        except BaseException as exc:
            raise_startup_error(exc)
    else:
        selected_port = args.port if args.port is not None else 8000
    args.port = selected_port
    if status_tracker:
        status_tracker.port = selected_port


if not os.path.exists(directory):
    raise_startup_error(FileExistsError(f"Directory '{directory}' does not exist"))
if not os.path.isdir(directory):
    raise_startup_error(FileExistsError(f"Directory '{directory}' is not a directory"))

if layout not in ("flat", "prefix"):
    raise_startup_error(RuntimeError("Layout must be 'flat' or 'prefix'"))

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


@app.middleware("http")
async def record_last_request(request: Request, call_next):
    if INACTIVITY_STATE["timeout"] is not None:
        INACTIVITY_STATE["last_request"] = time.monotonic()
    response = await call_next(request)
    if INACTIVITY_STATE["timeout"] is not None:
        INACTIVITY_STATE["last_request"] = time.monotonic()
    return response


@app.get("/has")
async def has_buffers(checksums: Annotated[List[Checksum], Body()]) -> JSONResponse:
    checksums2 = [parse_checksum(checksum) for checksum in checksums]
    curr_results = [0] * len(checksums)

    async def stat_all(paths):
        futures = []
        for _, path in paths:
            fut = anyio.Path(path).stat()
            futures.append(fut)
        result0 = await asyncio.gather(*futures, return_exceptions=True)
        for (nr, path), stat in zip(paths, result0):
            if isinstance(stat, Exception):
                continue
            curr_results[nr] = stat.st_size

    paths = []
    for nr, checksum in enumerate(checksums2):
        assert isinstance(checksum, str)
        if layout == "prefix":
            prefix = checksum[:2]
            path = os.path.join(directory, prefix, checksum)
        else:
            path = os.path.join(directory, checksum)
        paths.append((nr, path))

    await stat_all(paths)

    for extra_dir in extra_dirs:
        for nr, checksum in enumerate(checksums2):
            if curr_results[nr]:
                continue
            path = os.path.join(extra_dir, checksum)
            paths.append((nr, path))
        if not len(paths):
            break
        await stat_all(paths)

    return curr_results


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
    return response


_current_put_requests = set()


async def put_file(checksum: Annotated[Checksum, Path()], rq: Request) -> Response:

    if checksum in _current_put_requests:
        return Response(status_code=202)

    cs_stream = calculate_checksum_stream()

    if layout == "prefix":
        prefix = parse_checksum(checksum)[:2]
        path = os.path.join(directory, prefix, checksum)

    else:
        path = os.path.join(directory, checksum)

    if await aiofiles.ospath.exists(path):
        return Response(status_code=201)

    if layout == "prefix":
        target_directory = anyio.Path(os.path.join(directory, prefix))
        if not await target_directory.exists():
            await target_directory.mkdir(exist_ok=True)

    ok = False
    try:
        _current_put_requests.add(checksum)
        async with aiofiles.tempfile.NamedTemporaryFile(prefix=path + "-") as file:
            async for chunk in rq.stream():
                cs_stream.update(chunk)
                await file.write(chunk)
            buffer_checksum = cs_stream.hexdigest()
            if buffer_checksum != checksum:
                return Response(status_code=400, content="Incorrect checksum")
            if not await aiofiles.ospath.exists(path):
                try:
                    await aiofiles.os.link(file.name, path)
                except FileExistsError:
                    # someone else created the file, in the moment between .exists and .link
                    pass
            ok = True

    except ClientDisconnect:
        return Response(status_code=400)

    finally:
        _current_put_requests.remove(checksum)
        if not ok:
            try:
                pathlib.Path(path).unlink()
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

    config = uvicorn.Config(app, port=args.port, host=args.host)
    server = uvicorn.Server(config)

    if status_tracker:

        @app.on_event("startup")
        async def _hashserver_status_file_running():
            await anyio.to_thread.run_sync(status_tracker.write_running)

    if timeout_seconds is not None:
        setup_inactivity_timeout(timeout_seconds, server)

    print("OK")
    try:
        server.run()
    except BaseException:
        if status_tracker and not status_tracker.running_written:
            status_tracker.write_failed()
        raise
else:
    # uvicorn (or some other ASGI launcher) will take care of it
    pass

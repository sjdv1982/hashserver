import os
import sys
import argparse
from typing import Union, List
from hashlib import sha3_256

from fastapi import FastAPI, Path, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder

from functools import partial

from hash_file_response import parse_checksum, HashFileResponse, VaultHashFileResponse

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

    Buffers have the same file name as their (SHA3-256) checksum.
The directory may be organized as a Seamless vault directory,
containing subdirectories for (in)dependent and big/small buffers.""",
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

    parser.add_argument(
        "--port",
        type=int,
        help="Network port",
        default=8000,
    )

    parser.add_argument(
        "--host",
        type=str,
        help="Network host",
        default="127.0.0.1",
    )

    args = parser.parse_args()
    directory = args.directory
    lock_timeout = args.lock_timeout
    writable = args.writable

if not os.path.exists(directory):
    raise FileExistsError(f"Directory '{directory}' does not exist")
if not os.path.isdir(directory):
    raise FileExistsError(f"Directory '{directory}' is not a directory")
if lock_timeout <= 0:
    raise RuntimeError("Lock timeout must be positive")

is_vault = True
for dep in ("independent", "dependent"):
    for size in ("small", "big"):
        subdirectory = os.path.join(directory, dep, size)
        if not os.path.isdir(subdirectory):
            is_vault = False
            break

if is_vault and writable:
    raise RuntimeError("Vault directories cannot be writable")

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
async def has_buffers(checksums:Annotated[List[Checksum], Body()]) -> JSONResponse:
    global_lockpath = os.path.join(directory, ".LOCK")
    await until_no_lock(global_lockpath)
    
    result = []
    for checksum in checksums:
        path = os.path.join(directory, checksum)
        result.append(os.path.exists(path))
    return result

@app.get("/{checksum}")
async def get_file(checksum: Annotated[Checksum, Path()]) -> HashFileResponse:
    ResponseClass = VaultHashFileResponse if is_vault else HashFileResponse
    response = ResponseClass(directory=directory, checksum=checksum)
    response.lock_timeout = lock_timeout
    return response



async def put_file(checksum: Annotated[Checksum, Path()], rq: Request) -> Response:
    
    cs_stream = calculate_checksum_stream()

    path = os.path.join(directory, checksum)

    # Wait for locks from other applications
    global_lockpath = os.path.join(directory, ".LOCK")
    file_lockpath = path + ".LOCK"
    for lockpath in (global_lockpath, file_lockpath):
        await until_no_lock(lockpath)
    for lockpath in (global_lockpath, file_lockpath):
        try:
            pathlib.Path(lockpath).unlink()
        except FileNotFoundError:
            pass

    try:
        lock_touch_time = None
        async with await anyio.open_file(path, mode="wb") as file:
            async for chunk in rq.stream():
                cs_stream.update(chunk)
                if lock_touch_time is None or time.time() - lock_touch_time > 10:
                    pathlib.Path(file_lockpath).touch()
                    lock_touch_time = time.time()
                await file.write(chunk)
            buffer_checksum = cs_stream.hexdigest()
            if buffer_checksum != checksum:
                pathlib.Path(path).unlink()
                return Response(status_code=400, content="Incorrect checksum")
    finally:
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

if as_commandline_tool:
    import uvicorn

    uvicorn.run(app, port=args.port, host=args.host)
else:
    # uvicorn (or some other ASGI launcher) will take care of it
    pass

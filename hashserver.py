import os
import sys
import argparse
from typing import Union

from fastapi import FastAPI, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, JSONResponse
from fastapi.exceptions import RequestValidationError

from functools import partial

from hash_file_response import parse_checksum, HashFileResponse, VaultHashFileResponse

from typing_extensions import Annotated
from pydantic.functional_validators import BeforeValidator

Checksum = Annotated[
    Union[str, bytes], BeforeValidator(partial(parse_checksum, as_bytes=False))
]

DEFAULT_LOCK_TIMEOUT = 120.0
env = os.environ
as_commandline_tool = True
if "HASHSERVER_DIRECTORY" in os.environ:
    directory = os.environ["HASHSERVER_DIRECTORY"]
    lock_timeout = DEFAULT_LOCK_TIMEOUT
    if "HASHSERVER_LOCK_TIMEOUT" in os.environ:
        lock_timeout = float(os.environ["HASHSERVER_LOCK_TIMEOUT"])
    as_commandline_tool = False
else:
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

    args = parser.parse_args()
    directory = args.directory
    lock_timeout = args.lock_timeout

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

app = FastAPI()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    inner_exc = exc.args[0][0]
    inner_exc.pop("ctx", None)
    return JSONResponse(
        status_code=400,
        content={"message": "Invalid data", "exception": inner_exc},
    )

@app.exception_handler(FileNotFoundError)
async def filenotfound_exception_handler(request, exc):
    return Response(status_code=404, content="Not found")

@app.exception_handler(RuntimeError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"message": f"{exc}"},
    )


@app.get("/{checksum}")
async def get_file(checksum: Annotated[Checksum, Path()]) -> HashFileResponse:
    print("CDS!", checksum)
    ResponseClass = VaultHashFileResponse if is_vault else HashFileResponse
    response = ResponseClass(directory=directory, checksum=checksum)
    response.lock_timeout = lock_timeout
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if as_commandline_tool:
    import uvicorn

    uvicorn.run(app)
else:
    # uvicorn (or some other ASGI launcher) will take care of it
    pass

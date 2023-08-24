import os
import stat
import time
import typing
from hashlib import sha3_256

import anyio

from starlette.background import BackgroundTask
from starlette.types import Receive, Scope, Send
from starlette.responses import FileResponse


def parse_checksum(checksum, as_bytes=False):
    """Parses checksum and returns it as string.
    If as_bytes is True, return it as bytes instead.

    Snippet from the Seamless source code (fair use)"""
    if isinstance(checksum, bytes):
        checksum = checksum.hex()
    if isinstance(checksum, str):
        if len(checksum) % 2:
            raise ValueError("Wrong length")
        checksum = bytes.fromhex(checksum)

    if isinstance(checksum, bytes):
        if len(checksum) != 32:
            raise ValueError("Wrong length")
        if as_bytes:
            return checksum
        else:
            return checksum.hex()

    if checksum is None:
        return
    raise TypeError(type(checksum))


class HashFileResponse(FileResponse):
    """FileResponse with SHA3-256 checksum instead of filename.
    File has the same name as checksum."""

    lock_timeout = 120

    def __init__(
        self,
        checksum: str,
        directory: str,
        status_code: int = 200,
        headers: typing.Optional[typing.Mapping[str, str]] = None,
        media_type: typing.Optional[str] = None,
        background: typing.Optional[BackgroundTask] = None,
        stat_result: typing.Optional[os.stat_result] = None,
        method: typing.Optional[str] = None,
        content_disposition_type: str = "attachment",
    ) -> None:
        filename = parse_checksum(checksum)
        stat_result = None
        super().__init__(
            path=os.path.join(directory, filename),
            status_code=status_code,
            headers=headers,
            media_type=media_type,
            background=background,
            filename=filename,
            stat_result=stat_result,
            method=method,
            content_disposition_type=content_disposition_type,
        )
        self.directory = directory

    async def refresh_stat_headers(self):
        try:
            stat_result = await anyio.to_thread.run_sync(os.stat, self.path)
            del self.headers["content-length"]
            del self.headers["last-modified"]
            del self.headers["etag"]

            self.set_stat_headers(stat_result)
        except FileNotFoundError:
            raise FileNotFoundError(f"File at path {self.path} does not exist.")
        else:
            mode = stat_result.st_mode
            if not stat.S_ISREG(mode):
                raise RuntimeError(f"File at path {self.path} is not a file.")
        return stat_result

    async def until_no_lock(self):
        lockpaths = [os.path.join(self.directory, ".LOCK")] 
        if self.path is not None:
            lockpaths.append(self.path + ".LOCK")
        for lockpath in lockpaths:
            while 1:
                try:
                    lock_stat_result = await anyio.to_thread.run_sync(os.stat, lockpath)
                except FileNotFoundError:
                    break
                lock_mtime = lock_stat_result.st_mtime
                if time.time() - lock_mtime > self.lock_timeout:
                    break
                await anyio.sleep(1)

    async def calculate_checksum(self):
        hash = sha3_256()
        async with await anyio.open_file(self.path, mode="rb") as file:
            more_body = True
            while more_body:
                chunk = await file.read(self.chunk_size)
                hash.update(chunk)
                more_body = len(chunk) == self.chunk_size

            checksum = hash.digest().hex()
        return checksum

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if self.stat_result is None:
            try:
                stat_result = await self.refresh_stat_headers()
            except FileNotFoundError:
                await self.until_no_lock()
                stat_result = await self.refresh_stat_headers()
            self.stat_result = stat_result

        checksum = await self.calculate_checksum()
        if checksum != self.filename:
            await self.until_no_lock()
            stat_result = await self.refresh_stat_headers()
            self.stat_result = stat_result
            checksum2 = await self.calculate_checksum()
            if checksum2 != self.filename:
                raise RuntimeError(
                    f"File corruption: file at path {self.path} does not have the correct SHA3-256 checksum."
                )

        await super().__call__(scope=scope, receive=receive, send=send)


class VaultHashFileResponse(HashFileResponse):
    """FileResponse with SHA3-256 checksum instead of filename

    File has the same name as checksum.
    File exists inside a directory organized as a Seamless vault directory,
     containing subdirectories for (in)dependent and big/small buffers."""

    lock_timeout = 120

    def __init__(
        self,
        checksum: str,
        directory: str,
        status_code: int = 200,
        headers: typing.Optional[typing.Mapping[str, str]] = None,
        media_type: typing.Optional[str] = None,
        background: typing.Optional[BackgroundTask] = None,
        method: typing.Optional[str] = None,
        content_disposition_type: str = "attachment",
    ) -> None:
        filename = parse_checksum(checksum)
        FileResponse.__init__(
            self,
            path=None,
            status_code=status_code,
            headers=headers,
            media_type=media_type,
            background=background,
            filename=filename,
            stat_result=None,
            method=method,
            content_disposition_type=content_disposition_type,
        )
        self.directory = directory

    async def get_path(self):
        self.path = None
        for dep in ("independent", "dependent"):
            for size in ("small", "big"):
                path = os.path.join(self.directory, dep, size, self.filename)
                try:
                    stat_result = await anyio.to_thread.run_sync(os.stat, path)
                except FileNotFoundError:
                    continue

                mode = stat_result.st_mode
                if not stat.S_ISREG(mode):
                    raise RuntimeError(f"File at path {self.path} is not a file.")

                del self.headers["content-length"]
                del self.headers["last-modified"]
                del self.headers["etag"]

                self.stat_result = stat_result
                self.set_stat_headers(stat_result)
                self.path = path
                break
            if self.path is not None:
                break
        else:
            raise FileNotFoundError(f"File {self.filename} does not exist.")

    async def get_path_with_no_lock(self):
        last_path = None
        while 1:
            await self.until_no_lock()
            await self.get_path()
            if self.path == last_path:
                break
            last_path = self.path


    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await self.get_path()
        except FileNotFoundError:
            await self.get_path_with_no_lock()

        checksum = await self.calculate_checksum()
        if checksum != self.filename:
            await self.get_path_with_no_lock()
            checksum2 = await self.calculate_checksum()
            if checksum2 != self.filename:
                raise RuntimeError(
                    f"File corruption: file at path {self.path} does not have the correct SHA3-256 checksum."
                )

        await super().__call__(scope=scope, receive=receive, send=send)

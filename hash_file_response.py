import os
import stat
import time
import typing
import zlib
from hashlib import sha3_256, sha256

import anyio
import zstandard

from starlette.background import BackgroundTask
from starlette.types import Receive, Scope, Send
from starlette.responses import FileResponse

from compression_utils import (
    COMPRESSION_PREFERENCE,
    suffix_to_content_encoding,
)

HASH_ALGORITHMS = {
    "sha3-256": sha3_256,
    "sha-256": sha256,
}
DEFAULT_HASH_ALGORITHM = "sha-256"
_current_hash_algorithm = DEFAULT_HASH_ALGORITHM
_hash_constructor = HASH_ALGORITHMS[DEFAULT_HASH_ALGORITHM]


def set_hash_algorithm(algorithm: str) -> None:
    global _current_hash_algorithm, _hash_constructor
    try:
        _hash_constructor = HASH_ALGORITHMS[algorithm]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported hash algorithm '{algorithm}'. "
            f"Choose one of: {', '.join(HASH_ALGORITHMS)}"
        ) from exc
    _current_hash_algorithm = algorithm


def get_hash_algorithm() -> str:
    return _current_hash_algorithm


def parse_checksum(checksum) -> str:
    """Parses checksum and returns it as string.

    Adapted from the Seamless source code (fair use)"""
    if isinstance(checksum, bytes):
        checksum = checksum.hex()
    if isinstance(checksum, str):
        if len(checksum) % 2:
            raise ValueError("Wrong length")
        checksum = bytes.fromhex(checksum)

    if isinstance(checksum, bytes):
        if len(checksum) != 32:
            raise ValueError("Wrong length")
        return checksum.hex()

    if checksum is None:
        return
    raise TypeError(type(checksum))


class HashFileResponse(FileResponse):
    """FileResponse that validates files against their checksum-derived filename."""

    _PREFIX = False

    lock_timeout = 120
    chunk_size = 640 * 1024

    @staticmethod
    def _parse_accept_encoding(accept_encoding: str | None) -> list[str | None]:
        if not accept_encoding:
            return [None] + list(COMPRESSION_PREFERENCE)

        parsed: list[tuple[float, int, str | None]] = []
        for idx, item in enumerate(accept_encoding.split(",")):
            item = item.strip()
            if not item:
                continue
            parts = [part.strip() for part in item.split(";")]
            encoding = parts[0].lower()
            q = 1.0
            for param in parts[1:]:
                if param.startswith("q="):
                    try:
                        q = float(param[2:])
                    except ValueError:
                        q = 0.0
            if q <= 0:
                continue
            if encoding == "gzip":
                parsed.append((q, idx, ".gz"))
            elif encoding in ("zstd", "x-zstd"):
                parsed.append((q, idx, ".zst"))
            elif encoding == "identity":
                parsed.append((q, idx, None))
            elif encoding == "*":
                parsed.append((q, idx, "*"))

        parsed.sort(key=lambda item: (-item[0], item[1]))
        ordered: list[str | None] = []
        seen: set[str | None] = set()
        wildcard = False
        for _q, _idx, suffix in parsed:
            if suffix == "*":
                wildcard = True
                continue
            if suffix not in seen:
                seen.add(suffix)
                ordered.append(suffix)

        if wildcard:
            for suffix in [None, *COMPRESSION_PREFERENCE]:
                if suffix not in seen:
                    seen.add(suffix)
                    ordered.append(suffix)

        if None not in seen:
            ordered.append(None)
        for suffix in COMPRESSION_PREFERENCE:
            if suffix not in seen:
                ordered.append(suffix)
        return ordered

    def _compression_candidates(self, path: str) -> list[tuple[str, str | None]]:
        candidates = [(path, None)] + [
            (path + suffix, suffix) for suffix in COMPRESSION_PREFERENCE
        ]
        preferred_suffixes = self._parse_accept_encoding(self._accept_encoding)
        ordered_candidates: list[tuple[str, str | None]] = []
        seen: set[tuple[str, str | None]] = set()
        for suffix in preferred_suffixes:
            candidate = (path if suffix is None else path + suffix, suffix)
            if candidate not in seen:
                seen.add(candidate)
                ordered_candidates.append(candidate)
        for candidate in candidates:
            if candidate not in seen:
                seen.add(candidate)
                ordered_candidates.append(candidate)
        return ordered_candidates

    @staticmethod
    def _get_decompressor(suffix: str):
        if suffix == ".zst":
            return zstandard.ZstdDecompressor().decompressobj()
        if suffix == ".gz":
            return zlib.decompressobj(wbits=31)
        raise ValueError(suffix)

    def __init__(
        self,
        checksum: str,
        directory: str,
        status_code: int = 200,
        headers: typing.Optional[typing.Mapping[str, str]] = None,
        media_type: typing.Optional[str] = None,
        background: typing.Optional[BackgroundTask] = None,
        stat_result: typing.Optional[os.stat_result] = None,
        content_disposition_type: str = "attachment",
        extra_dirs: typing.Optional[typing.List[str]] = None,
        accept_encoding: typing.Optional[str] = None,
    ) -> None:
        filename = parse_checksum(checksum)
        self.prefix = filename[:2]
        stat_result = None
        if self._PREFIX:
            path = os.path.join(directory, self.prefix, filename)
        else:
            path = os.path.join(directory, filename)
        self._primary_path = path
        self._compression_suffix = None
        self._accept_encoding = accept_encoding
        super().__init__(
            path=path,
            status_code=status_code,
            headers=headers,
            media_type=media_type,
            background=background,
            filename=filename,
            stat_result=stat_result,
            content_disposition_type=content_disposition_type,
        )
        self.directory = directory
        self.extra_dirs = extra_dirs
        extra_dirs_layout = {}
        for extra_dir in extra_dirs:
            prefix_file = os.path.join(extra_dir, ".HASHSERVER_PREFIX")
            if os.path.exists(prefix_file):
                layout = "prefix"
            else:
                layout = "flat"
            extra_dirs_layout[extra_dir] = layout
        self.extra_dirs_layout = extra_dirs_layout

    async def _resolve_existing_path(
        self, base_path: str
    ) -> tuple[str | None, str | None]:
        for candidate_path, suffix in self._compression_candidates(base_path):
            if await anyio.Path(candidate_path).exists():
                return candidate_path, suffix
        return None, None

    async def refresh_stat_headers(self):
        resolved_path, compression_suffix = await self._resolve_existing_path(
            self._primary_path
        )
        if resolved_path is not None:
            self.path = resolved_path
            self._compression_suffix = compression_suffix
        elif self.extra_dirs:
            for extra_dir in self.extra_dirs:
                layout = self.extra_dirs_layout[extra_dir]
                if layout == "prefix":
                    base_path = os.path.join(extra_dir, self.prefix, self.filename)
                else:
                    base_path = os.path.join(extra_dir, self.filename)
                resolved_path, compression_suffix = await self._resolve_existing_path(
                    base_path
                )
                if resolved_path is not None:
                    self.path = resolved_path
                    self._compression_suffix = compression_suffix
                    break

        try:
            stat_result = await anyio.to_thread.run_sync(os.stat, self.path)
            del self.headers["content-length"]
            del self.headers["last-modified"]
            del self.headers["etag"]

            self.set_stat_headers(stat_result)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File at path {self.path} does not exist."
            ) from None
        else:
            mode = stat_result.st_mode
            if not stat.S_ISREG(mode):
                raise RuntimeError(f"File at path {self.path} is not a file.")
        return stat_result

    async def _until_no_lock(self, lockpaths):
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

    async def until_no_lock(self):
        lockpaths = [os.path.join(self.directory, ".LOCK")]
        if self.path is not None:
            lockpaths.append(self.path + ".LOCK")
        return await self._until_no_lock(lockpaths)

    async def calculate_checksum(self):
        """Return checksum for the configured algorithm."""
        checksum = _hash_constructor()
        async with await anyio.open_file(self.path, mode="rb") as file:
            more_body = True
            while more_body:
                chunk = await file.read(self.chunk_size)
                checksum.update(chunk)
                more_body = len(chunk) == self.chunk_size

            checksum = checksum.digest().hex()
        return checksum

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if self.stat_result is None:
            try:
                stat_result = await self.refresh_stat_headers()
            except FileNotFoundError:
                await self.until_no_lock()
                stat_result = await self.refresh_stat_headers()
            self.stat_result = stat_result

        if self._compression_suffix is not None:
            content_encoding = suffix_to_content_encoding(self._compression_suffix)
            if content_encoding is not None:
                self.headers["content-encoding"] = content_encoding

        if self._compression_suffix is None:
            checksum = await self.calculate_checksum()
            if checksum != self.filename:
                await self.until_no_lock()
                stat_result = await self.refresh_stat_headers()
                self.stat_result = stat_result
                checksum2 = await self.calculate_checksum()
                if checksum2 != self.filename:
                    raise RuntimeError(
                        f"File corruption: file at path {self.path} does not have the correct {_current_hash_algorithm} checksum."
                    )

        await super().__call__(scope=scope, receive=receive, send=send)


class PrefixHashFileResponse(HashFileResponse):
    """Same as HashFileResponse but files are stored under a two-character prefix.

    File has the same name as checksum.
    File is stored as $PREFIX/$CHECKSUM, where $PREFIX is the first two
      characters of $CHECKSUM
    """

    _PREFIX = True

    def __init__(
        self,
        checksum: str,
        directory: str,
        status_code: int = 200,
        headers: typing.Optional[typing.Mapping[str, str]] = None,
        media_type: typing.Optional[str] = None,
        background: typing.Optional[BackgroundTask] = None,
        stat_result: typing.Optional[os.stat_result] = None,
        content_disposition_type: str = "attachment",
        extra_dirs: typing.Optional[typing.List[str]] = None,
        accept_encoding: typing.Optional[str] = None,
    ) -> None:

        super().__init__(
            checksum=checksum,
            directory=directory,
            status_code=status_code,
            headers=headers,
            media_type=media_type,
            background=background,
            stat_result=stat_result,
            content_disposition_type=content_disposition_type,
            extra_dirs=extra_dirs,
            accept_encoding=accept_encoding,
        )
        prefix_file = os.path.join(directory, ".HASHSERVER_PREFIX")
        with open(prefix_file, mode="wb") as f:
            f.write(b"1\n")

    async def until_no_lock(self):
        lockpaths = [os.path.join(self.directory, self.prefix, ".LOCK")]
        if self.path is not None:
            lockpaths.append(self.path + ".LOCK")
        return await self._until_no_lock(lockpaths)

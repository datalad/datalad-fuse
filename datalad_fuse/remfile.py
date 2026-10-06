"""RemfileBackend — HDF5-optimised remote file access via the *remfile* library."""

from __future__ import annotations

import logging
import os.path
from pathlib import Path, PurePosixPath
import shutil
from types import ModuleType, TracebackType
from typing import IO, Any, Optional, cast
import urllib.request

from .backends import Backend
from .utils import AnnexKey

lgr = logging.getLogger("datalad.fuse.remfile")


def _get_remfile() -> Optional[ModuleType]:
    """Lazy import of remfile; returns None if not installed."""
    try:
        import remfile

        return remfile  # type: ignore[no-any-return]
    except ImportError:
        return None


class RemfileBackend(Backend):
    """Backend using remfile for HDF5-structured files (.nwb, .h5, etc.)."""

    name = "remfile"

    # File extensions this backend handles (HDF5-structured formats)
    EXTENSIONS = frozenset({".nwb", ".h5", ".hdf5", ".hdf", ".he5", ".nc", ".nc4"})

    #: Timeout, in seconds, for the size probe in :meth:`open_url`.
    PROBE_TIMEOUT = 10.0

    def __init__(self, path: str | Path, caching: bool) -> None:
        remfile_mod = _get_remfile()
        if remfile_mod is None:
            raise ImportError("remfile is not installed")
        self._remfile: ModuleType = remfile_mod
        self._cache_dir = os.path.join(path, ".git", "datalad", "cache", "remfile")
        # Without a disk cache, --caching=ondisk would silently be a no-op for
        # exactly the large files it matters most for.
        self._disk_cache = self._remfile.DiskCache(self._cache_dir) if caching else None

    def can_handle(
        self, key: Optional[AnnexKey], mode: str, relpath: Optional[str] = None
    ) -> bool:
        if mode != "rb":
            return False
        suffix = key.suffix if key is not None else None
        if suffix is None and relpath is not None:
            # URL/VURL keys (``addurl --fast``/``--relaxed``) carry no suffix,
            # so fall back to the name the user sees in the tree.
            suffix = PurePosixPath(relpath).suffix or None
        if suffix is None:
            return False
        return suffix.lower() in self.EXTENSIONS

    def _probe_size(self, url: str) -> Optional[int]:
        """Return the size of *url*, or None if the server did not say.

        ``remfile.File`` determines the size itself, but retries transient
        failures 8 times with exponential backoff (~25 s).  The adapter tries
        URLs one after another, and ``DataLadFUSE.open()`` holds the global
        rwlock while it does, so a single dead URL would stall an entire mount.
        Probing here instead keeps the fall-through to the next URL/backend as
        fast as it is for fsspec: one ranged request, one short timeout, no
        retries.
        """
        req = urllib.request.Request(url, headers={"Range": "bytes=0-0"})
        with urllib.request.urlopen(req, timeout=self.PROBE_TIMEOUT) as resp:
            # 206: "Content-Range: bytes 0-0/<total>" (<total> may be "*")
            content_range = resp.headers.get("Content-Range")
            if content_range is not None:
                total = content_range.rsplit("/", 1)[-1].strip()
                if total.isdigit():
                    return int(total)
            elif resp.status == 200:
                # Server ignored the Range header and sent the whole body
                content_length = resp.headers.get("Content-Length")
                if content_length is not None and content_length.isdigit():
                    return int(content_length)
        lgr.debug("Could not determine size of %s from probe response", url)
        return None

    def open_url(self, url: str, mode: str = "rb", **kwargs: Any) -> IO:  # noqa: U100
        if mode != "rb":
            # Mirror can_handle()'s mode check: remfile is binary-only, so
            # refuse text modes explicitly rather than silently returning a
            # binary stream when called directly (bypassing can_handle()).
            raise NotImplementedError(
                f"RemfileBackend only supports mode='rb', got {mode!r}"
            )
        size = self._probe_size(url)
        # `_size` is private API; see the remfile pin in setup.cfg.
        remfile_obj = self._remfile.File(url, _size=size, disk_cache=self._disk_cache)
        return cast(IO, RemfileWrapper(remfile_obj, url))

    def clear(self) -> None:
        if self._disk_cache is not None:
            shutil.rmtree(self._cache_dir, ignore_errors=True)


class RemfileWrapper:
    """Wraps ``remfile.File`` to satisfy the contracts expected by datalad-fuse.

    Adds context manager protocol, line iteration (for ``fsspec_head``), and an
    ``info()`` method compatible with ``file_getattr`` in *fuse_.py*.
    """

    _ITER_CHUNK = 8192
    # Hard cap on bytes returned per __next__ call.  Without this, iterating
    # a binary file (e.g. HDF5/NWB) — which has no '\n' — would download the
    # entire remote file in a single iteration step.
    _MAX_LINE_BYTES = 1 << 20  # 1 MiB

    def __init__(self, remfile_obj: Any, url: str) -> None:
        self._f = remfile_obj
        self._url = url
        self.closed = False

    def read(self, size: Optional[int] = -1) -> bytes:
        # ``remfile.RemFile.read()`` requires an explicit, non-negative size:
        # it returns b"" (and rewinds by one) for -1, never clamps at EOF, and
        # issues an unsatisfiable Range request when reading at EOF.  Clamp
        # here so the wrapper behaves like a regular file object.
        remaining = max(self._f.length - self._f.tell(), 0)
        if size is None or size < 0 or size > remaining:
            size = remaining
        if not size:
            return b""
        return self._f.read(size)  # type: ignore[no-any-return]

    def seek(self, offset: int, whence: int = 0) -> int:
        # ``remfile.RemFile.seek()`` returns None
        self._f.seek(offset, whence)
        return self.tell()

    def tell(self) -> int:
        return self._f.tell()  # type: ignore[no-any-return]

    def close(self) -> None:
        self._f.close()
        self.closed = True

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def __enter__(self) -> RemfileWrapper:
        return self

    def __exit__(
        self,
        _exc_type: Optional[type[BaseException]],
        _exc_val: Optional[BaseException],
        _exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def __iter__(self) -> RemfileWrapper:
        return self

    def __next__(self) -> bytes:
        chunks: list[bytes] = []
        total = 0
        while total < self._MAX_LINE_BYTES:
            chunk = self.read(self._ITER_CHUNK)
            if not chunk:
                if chunks:
                    return b"".join(chunks)
                raise StopIteration
            idx = chunk.find(b"\n")
            if idx != -1:
                chunks.append(chunk[: idx + 1])
                # Seek back past the bytes we read beyond the newline
                overshoot = len(chunk) - idx - 1
                if overshoot:
                    self._f.seek(-overshoot, 1)
                return b"".join(chunks)
            chunks.append(chunk)
            total += len(chunk)
        # Hit the cap without finding a newline — return what we have so the
        # caller still makes progress instead of OOM-ing on a binary file.
        return b"".join(chunks)

    def info(self) -> dict[str, Any]:
        """Minimal info dict matching the fsspec convention.

        ``DataLadFUSE.getattr(path, fh)`` reaches this for every ``fstat()`` on
        an open handle, so it must not hit the network: a HEAD request would
        fail outright against presigned S3 GET URLs, which reject HEAD.
        remfile already determined the size when the file was opened.
        """
        return {"type": "file", "size": self._f.length}

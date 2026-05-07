"""RemfileBackend — HDF5-optimised remote file access via the *remfile* library."""

from __future__ import annotations

import logging
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

    def __init__(self) -> None:
        remfile_mod = _get_remfile()
        if remfile_mod is None:
            raise ImportError("remfile is not installed")
        self._remfile: ModuleType = remfile_mod

    def can_handle(self, key: Optional[AnnexKey], mode: str) -> bool:
        if mode != "rb":
            return False
        if key is None or key.suffix is None:
            return False
        return key.suffix.lower() in self.EXTENSIONS

    def open_url(self, url: str, mode: str = "rb", **kwargs: Any) -> IO:  # noqa: U100
        return cast(IO, RemfileWrapper(self._remfile.File(url), url))


class RemfileWrapper:
    """Wraps ``remfile.File`` to satisfy the contracts expected by datalad-fuse.

    Adds context manager protocol, line iteration (for ``fsspec_head``), and an
    ``info()`` method compatible with ``file_getattr`` in *fuse_.py*.
    """

    _ITER_CHUNK = 8192

    def __init__(self, remfile_obj: Any, url: str) -> None:
        self._f = remfile_obj
        self._url = url
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        return self._f.read(size)  # type: ignore[no-any-return]

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._f.seek(offset, whence)  # type: ignore[no-any-return]

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
        while True:
            chunk = self._f.read(self._ITER_CHUNK)
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

    def info(self) -> dict[str, Any]:
        """Minimal info dict matching the fsspec convention.

        Issues a HEAD request to obtain ``Content-Length``.  This is rarely
        called in practice because HDF5 annex keys almost always carry size
        information.
        """
        req = urllib.request.Request(self._url, method="HEAD")
        with urllib.request.urlopen(req, timeout=30) as resp:
            content_length = resp.headers.get("Content-Length")
        if content_length is None:
            raise ValueError(
                f"HEAD response for {self._url} lacks Content-Length; "
                "cannot determine file size"
            )
        return {"type": "file", "size": int(content_length)}

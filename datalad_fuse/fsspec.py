"""FsspecBackend — remote file access via fsspec's HTTPFileSystem.

This module also provides backward-compatible imports for names that have
moved to :mod:`datalad_fuse.backends`, :mod:`datalad_fuse.remfile`, and
:mod:`datalad_fuse.adapter` as part of the multi-backend refactoring.
Importing those names from here still works but emits a
:class:`DeprecationWarning`.
"""

from __future__ import annotations

import importlib
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from typing import IO, Any, Optional
import warnings

import aiohttp
from aiohttp_retry import ListRetry, RetryClient
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.cached import CachingFileSystem
from fsspec.implementations.http import HTTPFileSystem

from .backends import Backend as _Backend
from .utils import AnnexKey

lgr = logging.getLogger("datalad.fuse.fsspec")


class FsspecBackend(_Backend):
    """Backend using fsspec's HTTPFileSystem (optionally with disk caching)."""

    name = "fsspec"

    def __init__(self, path: str | Path, caching: bool) -> None:
        fs = HTTPFileSystem(get_client=get_client)
        if caching:
            self.fs: HTTPFileSystem | CachingFileSystem = CachingFileSystem(
                fs=fs,
                cache_storage=os.path.join(path, ".git", "datalad", "cache", "fsspec"),
            )
        else:
            self.fs = fs
        self._caching = caching

    def can_handle(self, key: Optional[AnnexKey], mode: str) -> bool:  # noqa: U100
        return True  # fsspec handles everything

    def open_url(self, url: str, mode: str = "rb", **kwargs: Any) -> IO:
        try:
            return self.fs.open(url, mode, **kwargs)  # type: ignore[no-any-return]
        except BlocksizeMismatchError as e:
            lgr.warning(
                "Blocksize mismatch for %s: %s; clearing cache and retrying", url, e
            )
            self.fs.pop_from_cache(url)
            return self.fs.open(url, mode, **kwargs)  # type: ignore[no-any-return]

    def clear(self) -> None:
        if self._caching:
            self.fs.clear_cache()


# -- Async HTTP helpers (fsspec-specific) ------------------------------------


async def on_request_start(
    _session: aiohttp.ClientSession,
    trace_config_ctx: SimpleNamespace,
    params: aiohttp.TraceRequestStartParams,
) -> None:
    if trace_config_ctx.trace_request_ctx["current_attempt"] > 1:
        lgr.warning("Retrying request to %s", params.url)


async def get_client(**kwargs: Any) -> RetryClient:
    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    return RetryClient(
        client_session=aiohttp.ClientSession(
            trace_configs=[trace_config],
            **kwargs,
        ),
        retry_options=ListRetry(timeouts=[1, 2, 6, 15, 36]),
    )


# -- Backward compatibility --------------------------------------------------

_COMPAT_MAP: dict[str, tuple[str, str]] = {
    # Only names that existed in the pre-refactoring fsspec.py
    # name -> (module, canonical_name)
    "FsspecAdapter": ("datalad_fuse.adapter", "RemoteFilesystemAdapter"),
    "DatasetAdapter": ("datalad_fuse.adapter", "DatasetAdapter"),
    "FileState": ("datalad_fuse.adapter", "FileState"),
    "is_http_url": ("datalad_fuse.adapter", "is_http_url"),
}


def __getattr__(name: str) -> Any:
    if name in _COMPAT_MAP:
        module_path, canonical_name = _COMPAT_MAP[name]
        mod = importlib.import_module(module_path)
        obj = getattr(mod, canonical_name)
        warnings.warn(
            f"Importing {name!r} from 'datalad_fuse.fsspec' is deprecated. "
            f"Use 'from {module_path} import {canonical_name}' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return obj
    raise AttributeError(f"module 'datalad_fuse.fsspec' has no attribute {name!r}")

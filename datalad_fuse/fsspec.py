from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from enum import Enum
import logging
import os
import os.path
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import IO, Any, Optional, Tuple, cast

import aiohttp
from aiohttp_retry import ListRetry, RetryClient
from datalad.distribution.dataset import Dataset
from datalad.support.annexrepo import AnnexRepo
from datalad.utils import get_dataset_root
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.cached import CachingFileSystem
from fsspec.implementations.http import HTTPFileSystem
import methodtools

from .consts import CACHE_SIZE
from .utils import AnnexKey, is_annex_dir_or_key

lgr = logging.getLogger("datalad.fuse.fsspec")

FileState = Enum("FileState", "NOT_ANNEXED NO_CONTENT HAS_CONTENT")


class DatasetAdapter:
    def __init__(
        self, path: str | Path, caching: bool, mode_transparent: bool = False
    ) -> None:
        self.path = Path(path)
        self.mode_transparent = mode_transparent
        ds = Dataset(path)
        self.annex: Optional[AnnexRepo]
        if isinstance(ds.repo, AnnexRepo):
            self.annex = ds.repo
        else:
            self.annex = None
        self.commit_dt = datetime.fromtimestamp(
            ds.repo.get_commit_date(), tz=timezone.utc
        )
        self.caching = caching
        fs = HTTPFileSystem(get_client=get_client)
        if self.caching:
            self.fs = CachingFileSystem(
                fs=fs,
                # target_protocol='blockcache',
                cache_storage=os.path.join(path, ".git", "datalad", "cache", "fsspec"),
                # cache_check=600,
                # block_size=1024,
                # check_files=True,
                # expiry_times=True,
                # same_names=True
            )
        else:
            self.fs = fs

    def close(self) -> None:
        if self.annex is not None:
            self.annex._batched.clear()

    @methodtools.lru_cache(maxsize=CACHE_SIZE)
    def get_file_state(self, relpath: str) -> tuple[FileState, Optional[AnnexKey]]:
        p = self.path / relpath
        lgr.debug("get_file_state: %s", relpath)

        def handle_path_under_annex_objects(
            p: Path,
        ) -> tuple[FileState, Optional[AnnexKey]]:
            iadok = is_annex_dir_or_key(p)
            if isinstance(iadok, AnnexKey):
                if p.exists():
                    return (FileState.HAS_CONTENT, iadok)
                else:
                    return (FileState.NO_CONTENT, iadok)
            else:
                return (FileState.NOT_ANNEXED, None)

        # Shortcut handling of content under .git, in particular - annex key paths
        if self.mode_transparent and relpath.startswith(".git/"):
            return handle_path_under_annex_objects(p)

        # A regular file or git link for which we need to explicitly ask annex about
        if not p.is_symlink():
            if p.stat().st_size < 1024 and self.annex is not None:
                if self.annex.is_under_annex(relpath, batch=True):
                    key = AnnexKey.parse(self.annex.get_file_key(relpath, batch=True))
                    if self.annex.file_has_content(relpath, batch=True):
                        return (FileState.HAS_CONTENT, key)
                    else:
                        return (FileState.NO_CONTENT, key)
            return (FileState.NOT_ANNEXED, None)

        return handle_path_under_annex_objects(
            Path(os.path.normpath(p.parent / os.readlink(p)))
        )

    def get_urls(self, key: str) -> Iterator[str]:
        assert self.annex is not None
        # TODO: switch to batch=True whenever
        # https://github.com/datalad/datalad/pull/6379 is merged/released.
        # Will need a recent git-annex to work!
        whereis = self.annex.whereis(key, output="full", batch=False, key=True)
        remote_uuids = []
        for ru, v in whereis.items():
            remote_uuids.append(ru)
            for u in v["urls"]:
                if is_http_url(u):
                    yield u

        path_mixed = self.annex._batched.get(
            "examinekey",
            annex_options=["--format=annex/objects/${hashdirmixed}${key}/${key}\\n"],
            path=self.annex.path,
        )(key)
        path_lower = self.annex._batched.get(
            "examinekey",
            annex_options=["--format=annex/objects/${hashdirlower}${key}/${key}\\n"],
            path=self.annex.path,
        )(key)

        uuid2remote_url = {}
        for r in self.annex.get_remotes():
            ru = self.annex.config.get(f"remote.{r}.annex-uuid")
            if ru is None:
                continue
            remote_url = self.annex.config.get(f"remote.{r}.url")
            if remote_url is None:
                continue
            remote_url = self.annex.config.rewrite_url(remote_url)
            uuid2remote_url[ru] = remote_url

        for ru in remote_uuids:
            try:
                base_url = uuid2remote_url[ru]
            except KeyError:
                continue
            if is_http_url(base_url):
                if base_url.lower().rstrip("/").endswith("/.git"):
                    paths = [path_mixed, path_lower]
                else:
                    paths = [
                        path_lower,
                        path_mixed,
                        f".git/{path_lower}",
                        f".git/{path_mixed}",
                    ]
                for p in paths:
                    yield base_url.rstrip("/") + "/" + p

    def open(
        self,
        relpath: str,
        mode: str = "rb",
        encoding: str = "utf-8",
        errors: Optional[str] = None,
    ) -> IO:
        if mode not in ("r", "rb", "rt"):
            raise NotImplementedError("Only modes 'r', 'rb', and 'rt' are supported")
        if mode == "rb":
            kwargs = {}
        else:
            kwargs = {"encoding": encoding, "errors": errors}
        fstate, key = self.get_file_state(relpath)
        if fstate is FileState.NOT_ANNEXED:
            lgr.debug("%s: not under annex", relpath)
        else:
            lgr.debug(
                "%s: under annex, %s content",
                relpath,
                "has" if fstate is FileState.HAS_CONTENT else "does not have",
            )
        if fstate is FileState.NO_CONTENT:
            lgr.debug("%s: opening via fsspec", relpath)
            for url in self.get_urls(str(key)):
                try:
                    lgr.debug("%s: Attempting to open via URL %s", relpath, url)
                    return self.fs.open(url, mode, **kwargs)  # type: ignore
                except BlocksizeMismatchError as e:
                    lgr.warning(
                        "%s: Blocksize mismatch: %s; deleting cached file and"
                        " re-opening",
                        relpath,
                        e,
                    )
                    self.fs.pop_from_cache(url)
                    return self.fs.open(url, mode, **kwargs)  # type: ignore
                except FileNotFoundError as e:
                    lgr.debug(
                        "Failed to open file %s at URL %s: %s", relpath, url, str(e)
                    )
            raise IOError(
                f"Could not find a usable URL for {relpath} within {self.path}"
            )
        else:
            lgr.debug("%s: opening directly", relpath)
            return open(self.path / relpath, mode, **kwargs)  # type: ignore

    def clear(self) -> None:
        if self.caching:
            self.fs.clear_cache()


class FsspecAdapter:
    def __init__(
        self, root: str | Path, caching: bool, mode_transparent: bool = False
    ) -> None:
        self.root = Path(root)
        self.mode_transparent = mode_transparent
        self.caching = caching
        self.datasets: dict[Path, DatasetAdapter] = {}

    def __enter__(self) -> FsspecAdapter:
        return self

    def __exit__(
        self,
        _exc_type: Optional[type[BaseException]],
        _exc_val: Optional[BaseException],
        _exc_tb: Optional[TracebackType],
    ) -> None:
        for ds in self.datasets.values():
            ds.close()
        self.datasets.clear()

    @methodtools.lru_cache(maxsize=CACHE_SIZE)
    # TODO: optimize "caching" more since for all files under the same directory
    # they all would belong to the same dataset
    def get_dataset_path(self, path: str | Path) -> Path:
        path = Path(self.root, path)
        dspath = get_dataset_root(path)
        if dspath is None:
            raise ValueError(f"Path not under DataLad: {path}")
        dspath = Path(dspath)
        assert isinstance(dspath, Path)
        try:
            dspath.relative_to(self.root)
        except ValueError:
            raise ValueError(f"Path not under root dataset: {path}")
        return dspath

    def resolve_dataset(self, filepath: str | Path) -> tuple[DatasetAdapter, str]:
        dspath = self.get_dataset_path(filepath)
        try:
            dsap = self.datasets[dspath]
        except KeyError:
            dsap = self.datasets[dspath] = DatasetAdapter(
                dspath,
                mode_transparent=self.mode_transparent,
                caching=self.caching,
            )
        relpath = str(Path(filepath).relative_to(dspath))
        return dsap, relpath

    def open(
        self,
        filepath: str | Path,
        mode: str = "rb",
        encoding: str = "utf-8",
        errors: Optional[str] = None,
    ) -> IO:
        dsap, relpath = self.resolve_dataset(filepath)
        lgr.debug(
            "%s: path resolved to %s in dataset at %s", filepath, relpath, dsap.path
        )
        return dsap.open(relpath, mode=mode, encoding=encoding, errors=errors)

    def get_file_state(
        self, filepath: str | Path
    ) -> tuple[FileState, Optional[AnnexKey]]:
        dsap, relpath = self.resolve_dataset(filepath)
        return cast(Tuple[FileState, Optional[AnnexKey]], dsap.get_file_state(relpath))

    def is_under_annex(self, filepath: str | Path) -> bool:
        dsap, relpath = self.resolve_dataset(filepath)
        fstate, _ = dsap.get_file_state(relpath)
        return fstate is not FileState.NOT_ANNEXED

    def get_commit_datetime(self, filepath: str | Path) -> datetime:
        dsap, _ = self.resolve_dataset(filepath)
        return dsap.commit_dt


def is_http_url(s: str) -> bool:
    return s.lower().startswith(("http://", "https://"))


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

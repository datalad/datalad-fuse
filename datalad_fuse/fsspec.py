from enum import Enum
from functools import lru_cache
import logging
import os
import os.path
from pathlib import Path
from typing import IO, Dict, Iterator, Optional, Tuple, Union

from datalad.distribution.dataset import Dataset
from datalad.support.annexrepo import AnnexRepo
from datalad.utils import get_dataset_root
import fsspec
from fsspec.implementations.cached import CachingFileSystem

from .consts import CACHE_SIZE

lgr = logging.getLogger("datalad.fuse.fsspec")

FileState = Enum("FileState", "NOT_ANNEXED NO_CONTENT HAS_CONTENT")


class DatasetAdapter:
    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        ds = Dataset(path)
        self.annex: Optional[AnnexRepo]
        if isinstance(ds.repo, AnnexRepo):
            self.annex = ds.repo
        else:
            self.annex = None
        self.fs = CachingFileSystem(
            fs=fsspec.filesystem("http"),
            # target_protocol='blockcache',
            cache_storage=os.path.join(path, ".git", "datalad", "cache", "fsspec"),
            # cache_check=600,
            # block_size=1024,
            # check_files=True,
            # expiry_times=True,
            # same_names=True
        )

    def close(self) -> None:
        if self.annex is not None:
            self.annex._batched.clear()

    @lru_cache(maxsize=CACHE_SIZE)
    def get_file_state(self, relpath: str) -> Tuple[FileState, Optional[str]]:
        p = self.path / relpath
        if not p.is_symlink():
            if p.stat().st_size < 1024 and self.annex is not None:
                if self.annex.is_under_annex(relpath, batch=True):
                    key = self.annex.get_file_key(relpath, batch=True)
                    if self.annex.file_has_content(relpath, batch=True):
                        return (FileState.HAS_CONTENT, key)
                    else:
                        return (FileState.NO_CONTENT, key)
            return (FileState.NOT_ANNEXED, None)
        target = Path(os.path.normpath(p.parent / os.readlink(p)))
        try:
            target.relative_to(self.path / ".git" / "annex" / "objects")
        except ValueError:
            return (FileState.NOT_ANNEXED, None)
        key = target.name
        if target.exists():
            return (FileState.HAS_CONTENT, key)
        else:
            return (FileState.NO_CONTENT, key)

    def get_urls(self, filepath: Union[str, Path], key: str) -> Iterator[str]:
        assert self.annex is not None
        whereis = self.annex.whereis(str(filepath), output="full", batch=True)
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
            for url in self.get_urls(relpath, key):
                try:
                    lgr.debug("%s: Attempting to open via URL %s", relpath, url)
                    return self.fs.open(url, mode, **kwargs)
                except FileNotFoundError as e:
                    lgr.debug(
                        "Failed to open file %s at URL %s: %s", relpath, url, str(e)
                    )
            raise IOError(f"Could not find a usable URL for {relpath}")
        else:
            lgr.debug("%s: opening directly", relpath)
            return open(self.path / relpath, mode, **kwargs)

    def clear(self) -> None:
        self.fs.clear_cache()


class FsspecAdapter:
    def __init__(self, root: Union[str, Path]) -> None:
        self.root = Path(root)
        self.datasets: Dict[Path, DatasetAdapter] = {}

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        for ds in self.datasets.values():
            ds.close()
        self.datasets.clear()

    @lru_cache(maxsize=CACHE_SIZE)
    def get_dataset_path(self, path: Union[str, Path]) -> Path:
        path = Path(self.root, path)
        dspath = get_dataset_root(path)
        if dspath is None:
            raise ValueError(f"Path not under DataLad: {path}")
        dspath = Path(dspath)
        try:
            dspath.relative_to(self.root)
        except ValueError:
            raise ValueError(f"Path not under root dataset: {path}")
        return dspath

    def resolve_dataset(self, filepath: Union[str, Path]) -> Tuple[DatasetAdapter, str]:
        dspath = self.get_dataset_path(filepath)
        try:
            dsap = self.datasets[dspath]
        except KeyError:
            dsap = self.datasets[dspath] = DatasetAdapter(dspath)
        relpath = str(Path(filepath).relative_to(dspath))
        return dsap, relpath

    def open(
        self,
        filepath: Union[str, Path],
        mode: str = "rb",
        encoding: str = "utf-8",
        errors: Optional[str] = None,
    ) -> IO:
        dsap, relpath = self.resolve_dataset(filepath)
        lgr.debug(
            "%s: path resolved to %s in dataset at %s", filepath, relpath, dsap.path
        )
        return dsap.open(relpath, mode=mode, encoding=encoding, errors=errors)

    def is_under_annex(self, filepath: Union[str, Path]) -> bool:
        dsap, relpath = self.resolve_dataset(filepath)
        fstate, _ = dsap.get_file_state(relpath)
        return fstate is not FileState.NOT_ANNEXED


def is_http_url(s: str) -> bool:
    return s.lower().startswith(("http://", "https://"))

import logging
from pathlib import Path
from typing import IO, Iterator, Optional, Union

from datalad.support.annexrepo import AnnexRepo
import fsspec
from fsspec.implementations.cached import CachingFileSystem

lgr = logging.getLogger("datalad_fuse.fsspec")


class FsspecAdapter:
    def __init__(self, path: Union[str, Path]) -> None:
        self.annex = AnnexRepo(str(path))
        self.cache_dir = Path(path, ".git", "datalad", "cache", "fsspec")
        self.fs = CachingFileSystem(
            fs=fsspec.filesystem("http"),
            # target_protocol='blockcache',
            cache_storage=str(self.cache_dir),
            # cache_check=600,
            # block_size=1024,
            # check_files=True,
            # expiry_times=True,
            # same_names=True
        )

    def get_urls(self, filepath: Union[str, Path]) -> Iterator[str]:
        whereis = self.annex.whereis(str(filepath), output="full")
        remote_uuids = []
        for ru, v in whereis.items():
            remote_uuids.append(ru)
            for u in v["urls"]:
                if is_http_url(u):
                    yield u

        key = self.annex.get_file_key(filepath)
        path_mixed = self.annex.call_annex_oneline(
            [
                "examinekey",
                "--format=annex/objects/${hashdirmixed}${key}/${key}\\n",
                key,
            ]
        )
        path_lower = self.annex.call_annex_oneline(
            [
                "examinekey",
                "--format=annex/objects/${hashdirlower}${key}/${key}\\n",
                key,
            ]
        )

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
        filepath: Union[str, Path],
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
        under_annex = self.annex.is_under_annex(filepath)
        if under_annex:
            has_content = self.annex.file_has_content(filepath)
            lgr.debug(
                "%s: under annex, %s content",
                filepath,
                "has" if has_content else "does not have",
            )
        else:
            has_content = False
            lgr.debug("%s: not under annex", filepath)
        if under_annex and not has_content:
            lgr.debug("%s: opening via fsspec", filepath)
            for url in self.get_urls(filepath):
                try:
                    lgr.debug("%s: Attempting to open via URL %s", filepath, url)
                    return self.fs.open(url, mode, **kwargs)
                except FileNotFoundError as e:
                    lgr.debug(
                        "Failed to open file %s at URL %s: %s", filepath, url, str(e)
                    )
            raise IOError(f"Could not find a usable URL for {filepath}")
        else:
            lgr.debug("%s: opening directly", filepath)
            return open(filepath, mode, **kwargs)

    def clear(self) -> None:
        self.fs.clear_cache()


def is_http_url(s):
    return s.lower().startswith(("http://", "https://"))

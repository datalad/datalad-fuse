import logging
from pathlib import Path
from typing import IO, Iterator, Union

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
        for v in whereis.values():
            for u in v["urls"]:
                if u.lower().startswith(("http://", "https://")):
                    yield u

    def open(self, filepath: Union[str, Path], mode: str = "rb") -> IO:
        if mode != "rb":
            raise ValueError("'mode' must be 'rb'")
        if self.annex.is_under_annex(filepath) and not self.annex.file_has_content(
            filepath
        ):
            for url in self.get_urls(filepath):
                try:
                    return self.fs.open(url, mode)
                except FileNotFoundError as e:
                    lgr.debug(
                        "Failed to open file %s at URL %s: %s", filepath, url, str(e)
                    )
            raise IOError(f"Could not find a usable URL for {filepath}")
        else:
            return open(filepath, mode)

    def clear(self) -> None:
        self.fs.clear_cache()

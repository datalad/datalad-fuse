from pathlib import Path
from typing import IO, Union
from datalad.support.annexrepo import AnnexRepo
import fsspec
from fsspec.implementations.cached import CachingFileSystem


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

    def get_url(self, filepath: Union[str, Path]) -> str:
        whereis = self.annex.whereis(str(filepath), output="full")
        for v in whereis.values():
            if v["description"] == "web":
                for u in v["urls"]:
                    return u
        raise ValueError(f"No URL in git-annex for {filepath}")

    def open(self, filepath: Union[str, Path], mode: str = "rb") -> IO:
        if mode != "rb":
            raise ValueError("'mode' must be 'rb'")
        url = self.get_url(filepath)
        return self.fs.open(url, mode)

    def clear(self) -> None:
        self.fs.clear_cache()

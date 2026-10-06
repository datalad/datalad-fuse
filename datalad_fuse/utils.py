from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import re
from typing import Optional

from datalad_fuse.consts import CACHE_SIZE


@dataclass
class AnnexKey:
    """A git-annex key, parsed into its fields

    See <https://git-annex.branchable.com/internals/key_format/>.
    ``str()`` of an instance gives back the key.

    Examples
    --------
    >>> k = AnnexKey.parse("SHA256E-s1024--0123abcd.nwb")
    >>> k.backend, k.size, k.name, k.suffix
    ('SHA256E', 1024, '0123abcd', '.nwb')
    >>> str(k)
    'SHA256E-s1024--0123abcd.nwb'
    """

    backend: str
    name: str
    size: Optional[int] = None
    mtime: Optional[int] = None
    chunk_size: Optional[int] = None
    chunk_number: Optional[int] = None
    suffix: Optional[str] = None

    def __str__(self) -> str:
        s = self.backend
        if self.size is not None:
            s += f"-s{self.size}"
        if self.mtime is not None:
            s += f"-m{self.mtime}"
        if self.chunk_size is not None:
            s += f"-S{self.chunk_size}"
        if self.chunk_number is not None:
            s += f"-C{self.chunk_number}"
        s += f"--{self.name}"
        if self.suffix is not None:
            s += self.suffix
        return s

    @classmethod
    def parse(cls, s: str) -> AnnexKey:
        """Parse a key; raises `ValueError` if ``s`` is not a valid key"""
        m = re.fullmatch(
            r"(?P<backend>[A-Z0-9_]{2,14})"
            r"(?:-s(?P<size>[0-9]+))?"
            r"(?:-m(?P<mtime>[0-9]+))?"
            r"(?:-S(?P<chunk_size>[0-9]+)-C(?P<chunk_number>[0-9]+))?"
            r"--(?P<name>.+)",
            s,
        )
        if m:
            backend = m["backend"]
            size = int(m["size"]) if m["size"] is not None else None
            mtime = int(m["mtime"]) if m["mtime"] is not None else None
            chunk_size = int(m["chunk_size"]) if m["chunk_size"] is not None else None
            chunk_number = (
                int(m["chunk_number"]) if m["chunk_number"] is not None else None
            )
            name = m["name"]
            if backend.endswith("E"):
                name, sep, suffix = name.rpartition(".")
                suffix = sep + suffix
            else:
                suffix = None
            return cls(
                backend=backend,
                size=size,
                mtime=mtime,
                chunk_size=chunk_size,
                chunk_number=chunk_number,
                name=name,
                suffix=suffix,
            )
        else:
            raise ValueError(f"invalid git-annex key: {s!r}")

    @classmethod
    def parse_filename(cls, s: str) -> AnnexKey:
        """Parse a key from the name of a file under ``.git/annex/objects/``

        Such names escape some characters of the key (e.g. ``/`` as ``%``).
        """
        fields, sep, name = s.partition("--")
        # See `keyFile` and `fileKey` in `Annex/Locations.hs` in the git-annex
        # source
        name = (
            name.replace("%", "/")
            .replace("&c", ":")
            .replace("&s", "%")
            .replace("&a", "&")
        )
        return cls.parse(fields + sep + name)


@dataclass
class AnnexDir:
    """A (hashing) directory under ``.git/annex/objects/`` of a repository"""

    #: Top directory of the repository
    topdir: str


# might be called twice in rapid succession for an annex key path
@lru_cache(maxsize=CACHE_SIZE)
def is_annex_dir_or_key(path: str | Path) -> AnnexDir | AnnexKey | None:
    """Tell whether ``path`` points into ``.git/annex/objects/``

    Returns an `AnnexKey` for a key file
    (``.git/annex/objects/Xx/Yy/KEY/KEY``), an `AnnexDir` for a directory
    leading to one, and ``None`` otherwise.
    """
    parts = list(Path(path).parts)
    start = 0
    while True:
        try:
            i = parts.index(".git", start)
        except ValueError:
            return None
        if parts[i + 1 : i + 3] == ["annex", "objects"] and all(
            re.fullmatch(r"[A-Za-z0-9]{2}", p) for p in parts[i + 3 : i + 5]
        ):
            topdir = str(Path(*parts[:i]))
            depth = len(parts) - i
            if depth <= 5:  # have only two level of hash'ing directories
                return AnnexDir(topdir)
            try:
                key = AnnexKey.parse_filename(parts[i + 5])
            except ValueError:
                pass
            else:
                # note: key and its directory must match in name
                if depth == 7 and parts[-1] == parts[-2]:
                    return key
                elif depth == 6:
                    return AnnexDir(topdir)
        start = i + 1

from functools import lru_cache
from pathlib import Path
import re
from typing import Optional, Tuple, Union

from datalad_fuse.consts import CACHE_SIZE


# might be called twice in rapid succession for an annex key path
@lru_cache(maxsize=CACHE_SIZE)
def is_annex_dir_or_key(path: Union[str, Path]) -> Optional[Tuple[str, str]]:
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
                return (topdir, "dir")
            # matches an annex key regex in the form of
            # BACKEND[-sSIZE][-mMTIME][-Ssize-Cchunk]--HASH[EXTENSION]
            if re.fullmatch(
                r"[A-Z0-9_]{2,14}(?:-s[0-9]+)?(?:-m[0-9]+)?(?:-S[0-9]+-C[0-9]+)?--.*",
                parts[i + 5],
            ):
                # note: key and its directory must match in name
                if depth == 7 and parts[-1] == parts[-2]:
                    return (topdir, "key")
                elif depth == 6:
                    return (topdir, "dir")
        start = i + 1

from __future__ import annotations

from collections.abc import Callable
from ctypes import CDLL, c_int, c_void_p
from ctypes.util import find_library
from datetime import datetime
from errno import ENOENT, EROFS
from functools import wraps
import io
import logging
import os
import os.path as op
from pathlib import Path
import stat
import sys
from threading import Lock
from typing import IO, Any, Optional, TypeVar

from datalad import cfg
from datalad.distribution.dataset import Dataset
from fuse import FuseOSError, Operations
import methodtools

from .consts import CACHE_SIZE
from .fsspec import FsspecAdapter

# Make it relatively small since we are aiming for metadata records ATM
# Seems of no real good positive net ATM
# BLOCK_SIZE = 2**20  # 1M. block size to fetch at a time.
from .utils import AnnexDir, AnnexKey, is_annex_dir_or_key

if sys.version_info[:2] >= (3, 10):
    from typing import Concatenate, ParamSpec
else:
    from typing_extensions import Concatenate, ParamSpec

lgr = logging.getLogger("datalad.fuse")

libcname = find_library("c")
assert libcname is not None
libc = CDLL(libcname)

fcntl = libc.fcntl
fcntl.argtypes = [c_int, c_int, c_void_p]
fcntl.restype = c_int

T = TypeVar("T")
P = ParamSpec("P")


def write_op(
    f: Callable[Concatenate[DataLadFUSE, str, P], T]
) -> Callable[Concatenate[DataLadFUSE, str, P], T]:
    """Decorator for operations which need to write"""

    @wraps(f)
    def wrapped(self: DataLadFUSE, path: str, *args: P.args, **kwargs: P.kwargs) -> T:
        if self.mode_transparent and self.is_under_git(path):
            return f(self, path, *args, **kwargs)
        else:
            raise FuseOSError(EROFS)

    return wrapped


class DataLadFUSE(Operations):  # LoggingMixIn,
    # ??? TODO: since we would mix normal os.open
    # and not, we will mint our "fds" over this offset

    _counter_offset = 1000

    def __init__(
        self, root: str, caching: bool, mode_transparent: bool = False
    ) -> None:
        self.root = op.realpath(root)
        self.mode_transparent = mode_transparent
        self.rwlock = Lock()
        self._adapter = FsspecAdapter(
            root, mode_transparent=mode_transparent, caching=caching
        )
        self._fhdict: dict[int, Optional[IO[bytes]]] = {}
        # fh to fsspec_file, already opened (we are RO for now, so can just open
        # and there is no seek so we should be ok even if the same file open
        # multiple times?
        self._counter = DataLadFUSE._counter_offset

    def __call__(self, op: str, path: str, *args: Any) -> Any:
        lgr.debug("op=%s for path=%s with args %s", op, path, args)
        # if (".git", "annex", "objects") == Path(path).parts[-7:-4]:
        #     import pdb; pdb.set_trace()
        if not self.mode_transparent and ".git" in Path(path).parts:
            lgr.debug("Raising ENOENT for .git")
            raise FuseOSError(ENOENT)
        return super(DataLadFUSE, self).__call__(op, self.root + path, *args)

    def destroy(self, _path: Optional[str] = None) -> int:
        lgr.warning("Destroying fsspecs and collection of %d fhs", len(self._fhdict))
        for f in self._fhdict.values():
            if f is not None:
                try:
                    f.close()
                except Exception as e:
                    lgr.error("%s", e)
        self._fhdict = {}
        cache_clear = cfg.get("datalad.fusefs.cache-clear")
        if cache_clear == "visited":
            for dsap in self._adapter.datasets.values():
                dsap.clear()
        elif cache_clear == "recursive":
            Dataset(self.root).fsspec_cache_clear(recursive=True)
        return 0

    @staticmethod
    # XXX not yet sure what we need to filter...
    def _filter_stat(st: os.stat_result) -> dict[str, Any]:
        return dict(
            (key, getattr(st, key))
            for key in (
                "st_atime",
                "st_ctime",
                "st_gid",
                "st_mode",
                "st_mtime",
                "st_nlink",
                "st_size",
                "st_uid",
            )
        )

    @methodtools.lru_cache(maxsize=CACHE_SIZE)
    def getattr(self, path: str, fh: Optional[int] = None) -> dict[str, Any]:
        # TODO: support of unlocked files... but at what cost?
        lgr.debug("getattr(path=%r, fh=%r)", path, fh)
        r: Optional[dict[str, Any]] = None
        if fh and fh < self._counter_offset:
            lgr.debug("Calling os.fstat()")
            r = self._filter_stat(os.fstat(fh))
        elif op.exists(path):
            lgr.debug("File exists; calling os.stat()")
            r = self._filter_stat(os.stat(path))
        elif self.mode_transparent:
            if op.lexists(path):
                lgr.debug("Broken symlink; calling os.lstat()")
                r = self._filter_stat(os.lstat(path))
            else:
                iadok = is_annex_dir_or_key(path)
                if iadok is not None:
                    if isinstance(iadok, AnnexKey):
                        if iadok.size is not None:
                            lgr.debug("Got size from key")
                            r = mkstat(
                                is_file=True,
                                size=iadok.size,
                                timestamp=self._adapter.get_commit_datetime(path),
                            )
                        else:
                            # needs to be open but it is a key. We will let
                            # fsspec handle it
                            pass
                    elif isinstance(iadok, AnnexDir):
                        # just return that one of the top directory
                        # TODO: cache this since would be a frequent operation
                        r = self._filter_stat(os.stat(iadok.topdir))
                    else:
                        raise AssertionError(f"Unexpected iadok: {iadok!r}")
                elif self.is_under_git(path):
                    lgr.debug("Path under .git does not exist; raising ENOENT")
                    raise FuseOSError(ENOENT)
        if r is None:
            fsspec_file = None
            if fh and fh >= self._counter_offset:
                lgr.debug("File already open")
                fsspec_file = self._fhdict[fh]
                to_close = False
            else:
                _, key = self._adapter.get_file_state(path)
                assert key is not None
                if key.size is not None:
                    lgr.debug("Got size from key")
                    r = mkstat(
                        is_file=True,
                        size=key.size,
                        timestamp=self._adapter.get_commit_datetime(path),
                    )
                else:
                    lgr.debug("File not already open")
                    with self.rwlock:
                        fsspec_file = self._adapter.open(path)
                    to_close = True
            if fsspec_file is not None:
                if isinstance(fsspec_file, io.BufferedIOBase):  # type: ignore[unreachable]
                    # full file was already fetched locally
                    lgr.debug("File object is io.BufferedIOBase")  # type: ignore[unreachable]
                    r = self._filter_stat(os.stat(fsspec_file.name))
                else:
                    lgr.debug("File object is fsspec object")
                    r = file_getattr(
                        fsspec_file, timestamp=self._adapter.get_commit_datetime(path)
                    )
                if to_close:
                    with self.rwlock:
                        fsspec_file.close()
        lgr.debug("Returning %r for %s", r, path)
        assert r is not None
        return r

    def open(self, path: str, flags: int) -> int:
        lgr.debug("open(path=%r, flags=%#x)", path, flags)
        # fn = "".join([self.root, path.lstrip("/")])
        if op.exists(path) or (
            self.mode_transparent
            and self.is_under_git(path)
            and is_annex_dir_or_key(path) is None
        ):
            if op.exists(path):
                lgr.debug("Path exists; opening directly")
            else:
                lgr.debug("Path is under .git/; opening directly")
            fh = os.open(path, flags)
            if fh >= self._counter_offset:
                raise RuntimeError(
                    "We got file handle %d, our hopes that we never get such"
                    " high one were wrong" % fh
                )
            return fh
        else:
            lgr.debug("Opening path via fsspec")
            if flags % 2 == 0:
                # read
                mode = "rb"  # noqa: F841
            else:
                # write/create
                raise FuseOSError(EROFS)
            with self.rwlock:
                fsspec_file = self._adapter.open(path)
            lgr.debug("Counter = %d", self._counter)
            # TODO: threadlock ?
            self._fhdict[self._counter] = fsspec_file  # self.fs.open(fn, mode)
            self._counter += 1
            return self._counter - 1

    def read(self, _path: str, size: int, offset: int, fh: int) -> bytes:
        lgr.debug("read(path=%r, size=%r, offset=%r, fh=%r)", _path, size, offset, fh)
        if fh < self._counter_offset:
            lgr.debug("Reading directly")
            with self.rwlock:
                os.lseek(fh, offset, 0)
                return os.read(fh, size)
        else:
            lgr.debug("Reading from open filehandle")
            # must be open already and we must have mapped it to fsspec file
            # TODO: check for path to correspond?
            f = self._fhdict[fh]
            assert f is not None
            with self.rwlock:
                f.seek(offset)
                return f.read(size)

    def opendir(self, path: str) -> int:
        lgr.debug("opendir(path=%r)", path)
        if not op.exists(path):
            lgr.debug("Directory does not exist; raising ENOENT")
            raise FuseOSError(ENOENT)
        lgr.debug("Counter = %d", self._counter)
        # TODO: threadlock ?
        self._fhdict[self._counter] = None
        self._counter += 1
        return self._counter - 1

    def readdir(self, path: str, _fh: int) -> list[str]:
        lgr.debug("readdir(path=%r, fh=%r)", path, _fh)
        paths = [".", ".."] + os.listdir(path)
        if not self.mode_transparent:
            try:
                paths.remove(".git")
            except ValueError:
                pass
            else:
                lgr.debug("Removed .git from dirlist")
        return paths

    def release(self, path: str, fh: int) -> int:
        lgr.debug("release(path=%r, fh=%r)", path, fh)
        if fh < self._counter_offset:
            lgr.debug("Closing directly")
            os.close(fh)
        elif fh in self._fhdict:
            lgr.debug("Popping from filehandle collection")
            f = self._fhdict.pop(fh)
            # but we do not close an fsspec instance, so it could be reused
            # on subsequent accesses
            # TODO: this .close is not sufficient -- _fhdict is breeding open
            #  files, so we need to provide some proper use of lru_cache
            #  to have not recently used closed
            if f is not None and not f.closed:
                with self.rwlock:
                    f.close()
        return 0

    def readlink(self, path: str) -> str:
        lgr.debug("readlink(path=%r)", path)
        return os.readlink(path)

    # ??? seek seems to be not implemented by fusepy/ Operations

    #
    # Benign writeable operations which we can allow
    #
    mkdir = os.mkdir
    mknod = os.mknod
    rmdir = os.rmdir
    chmod = os.chmod
    chown = os.chown

    def flush(self, _path: str, fh: int) -> None:
        lgr.debug("flush(path=%r, fh=%r)", _path, fh)
        if fh < self._counter_offset:
            lgr.debug("Flushing directly")
            os.fsync(fh)

    def fsync(self, _path: str, datasync: int, fh: int) -> None:
        lgr.debug("fsync(path=%r, datasync=%r, fh=%r)", _path, datasync, fh)
        if fh < self._counter_offset:
            lgr.debug("Fsyncing directly")
            if datasync != 0:
                os.fdatasync(fh)  # type: ignore[attr-defined]
            else:
                os.fsync(fh)

    #
    # Extra operations we do not have implemented
    #
    getxattr = None
    listxattr = None

    ioctl = None

    # def access(self, path, mode):
    #     if not os.access(path, mode):
    #         raise FuseOSError(EACCES)
    #

    @write_op
    def create(self, path: str, mode: int) -> int:
        return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

    @write_op
    def link(self, target: str, source: str) -> None:
        if "/.git/" in source:
            os.link(self.root + source, target)
        else:
            raise FuseOSError(EROFS)

    @write_op
    def rename(self, old: str, new: str) -> None:
        if "/.git/" in new:
            os.rename(old, self.root + new)
        else:
            raise FuseOSError(EROFS)

    # def statfs(self, path: str) -> dict[str, Any]:
    #     lgr.mydebug(f"statfs {path}")
    #     raise NotImplementedError()
    #     stv = os.statvfs(path)
    #     return dict((key, getattr(stv, key)) for key in (
    #         'f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail',
    #         'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'))

    @write_op
    def symlink(self, target: str, source: str) -> None:
        os.symlink(source, target)

    @write_op
    def truncate(self, path: str, length: int, _fh: Optional[int] = None) -> None:
        with open(path, "r+") as f:
            f.truncate(length)

    @write_op
    def unlink(self, path: str) -> None:
        with self.rwlock:
            os.unlink(path)

    def utimens(self, path: str, times: Optional[tuple[int, int]] = None) -> None:
        if times is not None:
            os.utime(path, ns=times)
        else:
            os.utime(path)

    @write_op
    def write(self, _path: str, data: bytes, offset: int, fh: int) -> int:
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)

    @write_op
    def lock(self, _path: str, fh: int, cmd: int, lock: int) -> int:
        r = fcntl(fh, cmd, lock)
        assert isinstance(r, int)
        return r

    def is_under_git(self, path: str) -> bool:
        return ".git" in Path(path).relative_to(self.root).parts


def file_getattr(f: Any, timestamp: datetime) -> dict[str, Any]:
    # code borrowed from fsspec.fuse:FUSEr.getattr
    # TODO: improve upon! there might be mtime of url
    try:
        info = f.info()
    except FileNotFoundError:
        raise FuseOSError(ENOENT)
    return mkstat(info["type"] == "file", info["size"], timestamp)


def mkstat(is_file: bool, size: int, timestamp: datetime) -> dict[str, Any]:
    # TODO Also I get UID.GID funny -- yarik, not yoh
    # get of the original symlink, so float it up!
    data: dict[str, Any] = {"st_uid": os.getuid(), "st_gid": os.getgid()}
    if not is_file:
        data["st_mode"] = stat.S_IFDIR | 0o755
        data["st_size"] = 0
        data["st_blksize"] = 0
    else:
        data["st_mode"] = stat.S_IFREG | 0o644
        data["st_size"] = size
        data["st_blksize"] = 5 * 2**20
        data["st_nlink"] = 1
    data["st_atime"] = timestamp.timestamp()
    data["st_ctime"] = timestamp.timestamp()
    data["st_mtime"] = timestamp.timestamp()
    return data

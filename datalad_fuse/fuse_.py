from errno import ENOENT, EROFS
import io
import logging
import os
import os.path as op
from pathlib import Path
import stat
from threading import Lock
import time

from fuse import FuseOSError, Operations

from .fsspec import FsspecAdapter

# Make it relatively small since we are aiming for metadata records ATM
# Seems of no real good positive net ATM
# BLOCK_SIZE = 2**20  # 1M. block size to fetch at a time.

lgr = logging.getLogger("datalad.fuse")


def write_op(_f):
    """Decorator for operations which need to write

    We might not want them ATM
    """
    # TODO: allow rw
    return None


class DataLadFUSE(Operations):  # LoggingMixIn,
    # ??? TODO: since we would mix normal os.open
    # and not, we will mint our "fds" over this offset

    _counter_offset = 1000

    def __init__(self, root):
        self.root = op.realpath(root)
        self.rwlock = Lock()
        self._adapter = FsspecAdapter(root)
        self._cache = {}
        # fh to fsspec_file, already opened (we are RO for now, so can just open
        # and there is no seek so we should be ok even if the same file open
        # multiple times?
        self._counter = DataLadFUSE._counter_offset

    def __call__(self, op, path, *args):
        lgr.debug("op=%s for path=%s with args %s", op, path, args)
        if ".git" in Path(path).parts:
            lgr.debug("Raising ENOENT for .git")
            raise FuseOSError(ENOENT)
        return super(DataLadFUSE, self).__call__(op, self.root + path, *args)

    def destroy(self, _path=None):
        lgr.warning("Destroying fsspecs and cache of %d fhs", len(self._cache))
        for f in self._cache.values():
            try:
                f.close()
            except Exception as e:
                lgr.error("%s", e)
        self._cache = {}
        return 0

    @staticmethod
    # XXX not yet sure what we need to filter...
    def _filter_stat(st):
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

    def getattr(self, path, fh=None):
        # TODO: support of unlocked files... but at what cost?
        lgr.debug("getattr(path=%r, fh=%r)", path, fh)
        if fh and fh < self._counter_offset:
            lgr.debug("Calling os.fstat()")
            r = os.fstat(fh)
        elif op.exists(path):
            lgr.debug("File exists; calling os.stat()")
            r = self._filter_stat(os.stat(path))
        else:
            if fh and fh >= self._counter_offset:
                lgr.debug("File in cache")
                fsspec_file = self._cache[fh]
                to_close = False
            else:
                lgr.debug("File not in cache")
                fsspec_file = self._adapter.open(path)
                to_close = True
            if fsspec_file:
                if isinstance(fsspec_file, io.BufferedIOBase):
                    # full file was already fetched locally
                    lgr.debug("File object is io.BufferedIOBase")
                    r = self._filter_stat(os.stat(fsspec_file.name))
                else:
                    lgr.debug("File object is fsspec object")
                    r = file_getattr(fsspec_file)
                if to_close:
                    fsspec_file.close()
            else:
                # TODO: although seems to be logical -- seems to cause logging etc
                # lgr.error("ENOENTing %s %s", path, fh)
                # raise FuseOSError(ENOENT)
                lgr.debug("File failed to open???")
                r = {}  # we have nothing to say.  TODO: proper return/error?
        lgr.debug("Returning %r", r)
        return r

    def open(self, path, flags):
        lgr.debug("open(path=%r, flags=%#x)", path, flags)
        # fn = "".join([self.root, path.lstrip("/")])
        if op.exists(path):
            lgr.debug("Path exists; opening directly")
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
            fsspec_file = self._adapter.open(path)
            lgr.debug("Counter = %d", self._counter)
            # TODO: threadlock ?
            self._cache[self._counter] = fsspec_file  # self.fs.open(fn, mode)
            self._counter += 1
            return self._counter - 1

    def read(self, _path, size, offset, fh):
        lgr.debug("read(path=%r, size=%r, offset=%r, fh=%r)", _path, size, offset, fh)
        if fh < self._counter_offset:
            lgr.debug("Reading directly")
            with self.rwlock:
                os.lseek(fh, offset, 0)
                return os.read(fh, size)
        else:
            lgr.debug("Reading from cache")
            # must be open already and we must have mapped it to fsspec file
            # TODO: check for path to correspond?
            f = self._cache[fh]
            f.seek(offset)
            return f.read(size)

    def readdir(self, path, _fh):
        lgr.debug("readdir(path=%r, fh=%r)", path, _fh)
        paths = [".", ".."] + os.listdir(path)
        try:
            paths.remove(".git")
        except ValueError:
            pass
        else:
            lgr.debug("Removed .git from dirlist")
        return paths

    def release(self, path, fh):
        lgr.debug("release(path=%r, fh=%r)", path, fh)
        if fh < self._counter_offset:
            lgr.debug("Closing directly")
            os.close(fh)
        elif fh in self._cache:
            lgr.debug("Popping from cache")
            f = self._cache.pop(fh)
            # but we do not close an fsspec instance, so it could be reused
            # on subsequent accesses
            # TODO: this .close is not sufficient -- cache is breeding open
            #  files, so we need to provide some proper use of lru_cache
            #  to have not recently used closed
            if not f.closed:
                f.close()
        return 0

    def readlink(self, path):
        lgr.debug("readlink(path=%r)", path)
        linked_path = os.readlink(path)
        if self._adapter.is_under_annex(path):
            # TODO: we need all leading dirs to exist
            linked_path_full = op.join(op.dirname(path), linked_path)
            linked_path_dir = op.dirname(linked_path_full)
            if not op.exists(linked_path_dir):
                # TODO: this is just a hack - would lack proper permissions etc
                # and probably not needed per se!
                lgr.debug("Creating %s", linked_path_dir)
                os.makedirs(linked_path_dir)
        return linked_path

    # ??? seek seems to be not implemented by fusepy/ Operations

    #
    # Benign writeable operations which we can allow
    #
    mkdir = os.mkdir
    mknod = os.mknod
    rmdir = os.rmdir
    chmod = os.chmod
    chown = os.chown

    def flush(self, _path, fh):
        lgr.debug("flush(path=%r, fh=%r)", _path, fh)
        if fh < self._counter_offset:
            lgr.debug("Flushing directly")
            return os.fsync(fh)

    def fsync(self, _path, datasync, fh):
        lgr.debug("fsync(path=%r, datasync=%r, fh=%r)", _path, datasync, fh)
        if fh < self._counter_offset:
            lgr.debug("Fsyncing directly")
            if datasync != 0:
                return os.fdatasync(fh)
            else:
                return os.fsync(fh)

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

    #
    # Write operations we do not support anyhow ATM, but may be should?
    #
    @write_op
    def create(self, path, mode):
        return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

    @write_op
    def link(self, target, source):
        return os.link(self.root + source, target)

    @write_op
    def rename(self, old, new):
        return os.rename(old, self.root + new)

    # def statfs(self, path):
    #     lgr.mydebug(f"statfs {path}")
    #     raise NotImplementedError()
    #     stv = os.statvfs(path)
    #     return dict((key, getattr(stv, key)) for key in (
    #         'f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail',
    #         'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'))

    @write_op
    def symlink(self, target, source):
        return os.symlink(source, target)

    @write_op
    def truncate(self, path, length, _fh=None):
        with open(path, "r+") as f:
            f.truncate(length)

    unlink = write_op(os.unlink)
    utimens = os.utime

    @write_op
    def write(self, _path, data, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


def file_getattr(f):
    # code borrowed from fsspec.fuse:FUSEr.getattr
    # TODO: improve upon! there might be mtime of url
    try:
        info = f.info()
    except FileNotFoundError:
        raise FuseOSError(ENOENT)
    # TODO Also I get UID.GID funny -- yarik, not yoh
    # get of the original symlink, so float it up!
    data = {"st_uid": os.getuid(), "st_gid": os.getgid()}
    if info["type"] != "file":
        data["st_mode"] = stat.S_IFDIR | 0o755
        data["st_size"] = 0
        data["st_blksize"] = 0
    else:
        data["st_mode"] = stat.S_IFREG | 0o644
        data["st_size"] = info["size"]
        data["st_blksize"] = 5 * 2 ** 20
        data["st_nlink"] = 1
    data["st_atime"] = time.time()
    data["st_ctime"] = time.time()
    data["st_mtime"] = time.time()
    return data

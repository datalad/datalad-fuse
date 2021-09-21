from __future__ import print_function, absolute_import, division

import io
import logging
import os
import os.path as op
import re
import stat
import time

from itertools import chain
from functools import lru_cache

from errno import EACCES
from os.path import realpath
from threading import Lock

from functools import wraps

import fsspec
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from datalad.support.annexrepo import AnnexRepo
from datalad import cfg

CACHE_DIR = op.join(cfg.obtain('datalad.locations.cache'), 'fuse')

# explicit blockcache instance for better control etc
import fsspec.implementations.cached
fs_block = fsspec.implementations.cached.CachingFileSystem(
        fs=fsspec.filesystem('http'), # , target_protocol='blockcache'),
        #target_protocol='blockcache',
        cache_storage=CACHE_DIR,
        #cache_check=600,
        #block_size=1024,
        #check_files=True,
        #expiry_times=True,
        #same_names=True
        )

from errno import (
    ENOENT,
    EROFS,
)

# well -- in principle the key should also be repeated twice
# now would also match directory itself
# TODO: add lookahead or behind to match?
ANNEX_KEY_PATH_REGEX = re.compile(
    '(?P<repo_path>.*)\.git/annex/objects/.*/'
    '(?P<key>'
     '(?P<backend>[^-]+)-'
     '(?P<maybesize>[^-]+)--[^/]*)$'
)

# Make it relatively small since we are aiming for metadata records ATM
# Seems of no real good positive net ATM
#BLOCK_SIZE = 2**20  # 1M. block size to fetch at a time.

lgr = logging.getLogger('datalad.fuse')


def write_op(f):
    """Decorator for operations which need to write

    We might not want them ATM
    """
    # TODO: allow rw
    return None


@lru_cache(None)
def _get_annex_repo_key(path):
    path = op.realpath(path)
    res = ANNEX_KEY_PATH_REGEX.search(path)
    if res:
        return res['repo_path'], res['key']
    else:
        return None, None


class fsspecFiles:

    def __init__(self):
        self._files = {}

    def close(self):
        for f in self._files.values():
            f.close()
        self._files = {}

    @classmethod
    def file_getattr(cls, f):
        # code borrowed from fsspec.fuse:FUSEr.getattr
        # TODO: improve upon! there might be mtime of url
        try:
            info = f.info()
        except FileNotFoundError:
            raise FuseOSError(ENOENT)
        # TODO Also I get UID.GID funny -- yarik, not yoh
        # get of the original symlink, so float it up!
        data = {"st_uid": 1000, "st_gid": 1000}
        perm = 0o777

        if info["type"] != "file":
            data["st_mode"] = stat.S_IFDIR | perm
            data["st_size"] = 0
            data["st_blksize"] = 0
        else:
            data["st_mode"] = stat.S_IFREG | perm
            data["st_size"] = info["size"]
            data["st_blksize"] = 5 * 2 ** 20
            data["st_nlink"] = 1
        data["st_atime"] = time.time()
        data["st_ctime"] = time.time()
        data["st_mtime"] = time.time()
        return data

    @lru_cache(1024)   # under assumption that we are in truly read-only mode
    # may be add fscache'ing?
    def _get_url(self, path):
        annex_repo, annex_key = _get_annex_repo_key(path)
        if annex_key:
            # so we do not have it yet!
            repo = AnnexRepo(annex_repo)
            whereis = repo.whereis(annex_key, output='full', key=True) # , batch=True)
            # TODO: support also regular http remotes etc
            urls = list(chain(*(x.get('urls', []) for x in whereis.values())))
            # TODO: some kind of analysis/fallback and not just taking the first one
            # TODO: Opened connections already might even have already some "equivalent" URL
            if urls:
                return urls[0]

    # TODO: add proper caching and "expunging" of those fsspec instances?
    def _get_file(self, path):
        """Given an annexed (full) path return an open fsspec File

        path must be a symlink, no unlocked files or funny file systems
        are supported ATM. (TODO for those who care, but I should make benchmarks first)

        If that File (for a url) is not yet known, it will open a
        new one with the blockcache.

        It would return None if no URL for a path
        """
        url = self._get_url(path)
        if url:
            if url in self._files:
                f = self._files[url]
            else:
                #f = fsspec.open(
                #    f"blockcache::{url}",
                #    blockcache={'cache_storage': CACHE_DIR}  # TODO
                #)
                self._files[url] = f = fs_block.open(url) # , block_size=BLOCK_SIZE)
            if f.closed:
                f.open()
            return f


class DataLadFUSE(Operations):  # LoggingMixIn,
    # ??? TODO: since we would mix normal os.open
    # and not, we will mint our "fds" over this offset

    _counter_offset = 1000

    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()
        self._fsspec_files = fsspecFiles()
        self._cache = {}
        # fh to fsspec_file, already opened (we are RO for now, so can just open
        # and there is no seek so we should be ok even if the same file open
        # multiple times?
        self._counter = DataLadFUSE._counter_offset

    def __call__(self, op, path, *args):
        lgr.log(5, "op=%s for path=%s with args %s", op, path, args)
        return super(DataLadFUSE, self).__call__(op, self.root + path, *args)

    def destroy(self, path=None):
        lgr.warning("Destroying fsspecs and cache of %d fhs", len(self._cache))
        try:
            self._fsspec_files.close()
            self._cache = {}
        except Exception as e:
            lgr.error(e)
        return 0

    @staticmethod
    # XXX not yet sure what we need to filter...
    def _filter_stat(st):
        return dict((key, getattr(st, key)) for key in (
            'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime',
            'st_nlink', 'st_size', 'st_uid'))

    def getattr(self, path, fh=None):
        # TODO: support of unlocked files... but at what cost?
        if (fh and fh < self._counter_offset):
            return os.fstat(fh)
        elif op.exists(path):
            return self._filter_stat(os.stat(path))
        else:
            if fh and fh >= self._counter_offset:
                fsspec_file = self._cache[fh]
            else:
                fsspec_file = self._fsspec_files._get_file(path)
            if fsspec_file:
                if isinstance(fsspec_file, io.BufferedIOBase):
                   # full file was already fetched locally
                    return self._filter_stat(os.stat(fsspec_file.name))
                else:
                    return fsspecFiles.file_getattr(fsspec_file)
            # TODO: although seems to be logical -- seems to cause logging etc
            # lgr.error("ENOENTing %s %s", path, fh)
            # raise FuseOSError(ENOENT)
            return {}  # we have nothing to say.  TODO: proper return/error?

    def open(self, path, flags):
        #fn = "".join([self.root, path.lstrip("/")])
        if op.exists(path):
            fh = os.open(path, flags)
            if fh >= self._counter_offset:
                raise RuntimeError("We got file handle %d, our hopes that we never get such high one were wrong" % fh)
            return fh
        else:
            if flags % 2 == 0:
                # read
                mode = "rb"
            else:
                # write/create
                raise FuseOSError(EROFS)
            fsspec_file = self._fsspec_files._get_file(path)
            # TODO: threadlock ?
            self._cache[self._counter] = fsspec_file # self.fs.open(fn, mode)
            self._counter += 1
            return self._counter - 1

    def read(self, path, size, offset, fh):
        if fh < self._counter_offset:
            with self.rwlock:
                os.lseek(fh, offset, 0)
                return os.read(fh, size)
        else:
            # must be open already and we must have mapped it to fsspec file
            # TODO: check for path to correspond?
            f = self._cache[fh]
            f.seek(offset)
            return f.read(size)

    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(path)

    def release(self, path, fh):
        lgr.debug("Closing for %s fh=%d", path, fh)
        if fh < self._counter_offset:
            os.close(fh)
        elif fh in self._cache:
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
        linked_path = os.readlink(path)
        repo_path, annex_key = _get_annex_repo_key(linked_path)
        if annex_key:
            # TODO: we need all leading dirs to exist
            linked_path_full = op.join(op.dirname(path), linked_path)
            linked_path_dir = op.dirname(linked_path_full)
            if not op.exists(linked_path_dir):
                # TODO: this is just a hack - would lack proper permissions etc
                # and probably not needed per se!
                lgr.debug("Creating %s", linked_path_dir)
                os.makedirs(linked_path_dir)
        return linked_path

    #??? seek seems to be not implemented by fusepy/ Operations

    #
    # Benign writeable operations which we can allow
    #
    mkdir = os.mkdir
    mknod = os.mknod
    rmdir = os.rmdir
    chmod = os.chmod
    chown = os.chown


    def flush(self, path, fh):
        if fh < self._counter_offset:
            return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        if fh < self._counter_offset:
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
    def truncate(self, path, length, fh=None):
        with open(path, 'r+') as f:
            f.truncate(length)

    unlink = write_op(os.unlink)
    utimens = os.utime

    @write_op
    def write(self, path, data, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('root')
    parser.add_argument('mount')
    args = parser.parse_args()

    fuse = FUSE(
        DataLadFUSE(args.root), args.mount, foreground=True
        # , allow_other=True
        )

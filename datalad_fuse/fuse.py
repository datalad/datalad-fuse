from __future__ import print_function, absolute_import, division

import logging
import os
import os.path as op
import re

from itertools import chain
from functools import lru_cache

from errno import EACCES
from os.path import realpath
from threading import Lock

from functools import wraps

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from datalad.support.annexrepo import AnnexRepo


# well -- in principle the key should also be repeated twice
# now would also match directory itself
# TODO: add lookahead or behind to match?
ANNEX_KEY_PATH_REGEX = re.compile(
    '(?P<repo_path>.*)\.git/annex/objects/.*/'
    '(?P<key>'
     '(?P<backend>[^-]+)-'
     '(?P<maybesize>[^-]+)--[^/]*)$'
)

lgr = LoggingMixIn.log
lgr.mydebug = lambda *args: lgr.info("MINE: " + args[0], *args[1:])


def write_op(f):
    """Decorator for operations which need to write

    We might not want them ATM
    """
    # TODO: allow rw
    return None

@lru_cache
def _get_annex_repo_key(path):
    res = ANNEX_KEY_PATH_REGEX.search(path)
    if res:
        return res['repo_path'], res['key']
    else:
        return None, None

class RCloneFuser:
    def __init__(self):
        self._rclone_proc = None

    def close(self):
        if self._rclone_proc:
            self._rclone_proc.terminate()
            self._rclone_proc = None


class RCloneFusers:
    def __init__(self):
        self._fusers = []

    def close(self):
        for fuser in self._fusers:
            fuser.close()


    _url_schemas = {
        'https?://s3.amazonaws.com/(?P<bucket>[^/]+)/(?P<path>[^?#]*)([#?&](?P<extra>.*))': "s3",
        'https?://(?P<bucket>[^/]+)\.s3.amazonaws.com/(?P<path>[^?#]*)([#?&](?P<extra>.*))': "s3",
        # TODO: catch all http
    }


    # TODO: move into a separate helper class?
    def _get_fused_url(self, url):
        """Given a URL use rclone to "mount" it"""
        lgr.info("Fusing %s", url)
        for regex, type_ in self._url_schemas.items():
            match = re.match(regex, url)
            if match:
                # we got the winner
                if type_ == 's3':

                    fuser_name = 's3_'
                else:
                    raise NotImplementedError(type_)

    # @lru_cache   # under assumption that we are in truly read-only mode
    def _get_fused_path(self, path):
        """Given a path return a path on rclone FUSE filesystem

        where rclone would take care about making it sparse etc,
        and we will just proxy all the requests to that path
        """
        annex_repo, annex_key = _get_annex_repo_key(path)
        if annex_key:
            # so we do not have it yet!
            repo = AnnexRepo(annex_repo)
            whereis = repo.whereis(annex_key, output='full', key=True, batch=True)
            # TODO: support also regular http remotes etc
            urls = list(chain(*(x.get('urls', []) for x in whereis.values())))
            # TODO: some kind of analysis/fallback and not just taking the first one
            if urls:
                url = urls[0]
                return self._get_fused_url(url)


class Loopback(LoggingMixIn, Operations):
    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()
        self._rclone_fusers = RCloneFusers()

    def __call__(self, op, path, *args):
        return super(Loopback, self).__call__(op, self.root + path, *args)

    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    chmod = write_op(os.chmod)
    chown = write_op(os.chown)

    @write_op
    def create(self, path, mode):
        return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        if datasync != 0:
            return os.fdatasync(fh)
        else:
            return os.fsync(fh)

    def getattr(self, path, fh=None):
        try:
            st = os.lstat(path)
            return dict((key, getattr(st, key)) for key in (
                'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime',
                'st_nlink', 'st_size', 'st_uid'))
        except:
            fused_path = self._rclone_fusers._get_fused_path(path)
            if fused_path:
                return self.getattr(fused_path)
            return {}  # we have nothing to say.  TODO: proper return/error?

    getxattr = None

    @write_op
    def link(self, target, source):
        return os.link(self.root + source, target)

    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod
    open = os.open

    def read(self, path, size, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(path)

    #readlink = os.readlink
    def readlink(self, path):
        linked_path = os.readlink(path)
        repo_path, annex_key = _get_annex_repo_key(linked_path)
        if annex_key:
            # TODO: we need all leading dirs to exist
            linked_path_full = op.join(op.dirname(path), linked_path)
            linked_path_dir = op.dirname(linked_path_full)
            if not op.exists(linked_path_dir):
                # TODO: this is just a hack - would lack proper permissions etc
                lgr.mydebug(f"Creating {linked_path_dir}")
                os.makedirs(linked_path_dir)
        return linked_path

    def release(self, path, fh):
        return os.close(fh)

    @write_op
    def rename(self, old, new):
        return os.rename(old, self.root + new)

    rmdir = write_op(os.rmdir)

    def statfs(self, path):
        lgr.mydebug(f"statfs {path}")
        raise NotImplementedError()
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in (
            'f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail',
            'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'))

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

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(
        Loopback(args.root), args.mount, foreground=True
        # , allow_other=True
        )

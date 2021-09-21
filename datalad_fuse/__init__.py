"""DataLad FUSE extension"""

__docformat__ = 'restructuredtext'

from typing import Any, Dict, Iterator, Optional

from os.path import curdir
from os.path import abspath

from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.support.param import Parameter
from datalad.distribution.dataset import datasetmethod
from datalad.interface.utils import eval_results
from datalad.support.constraints import EnsureChoice, EnsureNone
from datalad.distribution.dataset import (
    Dataset, EnsureDataset, require_dataset,
)

from datalad.interface.results import get_status_dict

from fuse import FUSE
from .fuse_ import DataLadFUSE

# defines a datalad command suite
# this symbold must be indentified as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "DataLad FUSE command suite",
    [
        # specification of a command, any number of commands can be defined
        (
            # importable module that contains the command implementation
            'datalad_fuse',
            # name of the command class implementation in above module
            'FuseFS',
            # optional name of the command in the cmdline API
            'fusefs',
            # optional name of the command in the Python API
            'fusefs'
        ),
        ("datalad_fuse.fsspec_head", "FsspecHead", "fsspec-head", "fsspec_head"),
        (
            "datalad_fuse.fsspec_cache_clear",
            "FsspecCacheClear",
            "fsspec-cache-clear",
            "fsspec_cache_clear",
        ),
    ]
)


# decoration auto-generates standard help
@build_doc
# all commands must be derived from Interface
class FuseFS(Interface):
    # first docstring line is used a short description in the cmdline help
    # the rest is put in the verbose help and manpage
    """FUSE File system providing transparent access to files under DataLad control

    """

    # parameters of the command, must be exhaustive
    _params_ = {
        "dataset": Parameter(
            args=("-d", "--dataset"),
            doc="""dataset to operate on.  If no dataset is given, an
                attempt is made to identify the dataset based on the current
                working directory.""",
            constraints=EnsureDataset() | EnsureNone(),
        ),
        "mount_path": Parameter(
            args=("mount_path",),
            metavar="PATH",
            doc="""Path where to mount the dataset (should exist).""",
        ),
        # TODO: (might better become config vars?)
        # --cache=persist
        # --recursive=follow,get - encountering submodule might install it first
        # --git=[hide],show - hide .git in the FUSE space to avoid confusion/etc
    }

    @staticmethod
    # decorator binds the command to the Dataset class as a method
    @datasetmethod(name='fusefs')
    # generic handling of command results (logging, rendering, filtering, ...)
    @eval_results
    # signature must match parameter list above
    # additional generic arguments are added by decorators
    def __call__(mount_path: str, dataset: Optional[Dataset] = None) -> Iterator[Dict[str, Any]]:

        ds = require_dataset(
            dataset, purpose="clear fsspec cache", check_installed=True
        )

        fuse = FUSE(
            DataLadFUSE(ds.path), mount_path, foreground=True
            # , allow_other=True
        )

        # commands should be implemented as generators and should
        # report any results by yielding status dictionaries
        yield get_status_dict(
            # an action label must be defined, the command name make a good
            # default
            action='fusefs',
            # most results will be about something associated with a dataset
            # (component), reported paths MUST be absolute
            path=mount_path,
            # status labels are used to identify how a result will be reported
            # and can be used for filtering
            status='ok',
            # arbitrary result message, can be a str or tuple. in the latter
            # case string expansion with arguments is delayed until the
            # message actually needs to be rendered (analog to exception messages)
            message=msg)


from datalad import setup_package
from datalad import teardown_package

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

"""DataLad FUSE extension"""

from __future__ import annotations

__docformat__ = "restructuredtext"

from typing import Any, Dict, Iterator, Optional

from datalad.distribution.dataset import (
    Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.interface.base import Interface, build_doc, eval_results
from datalad.interface.results import get_status_dict
from datalad.support.constraints import EnsureNone
from datalad.support.param import Parameter

from ._version import get_versions

# Defines a datalad command suite.
# This variable must be bound as a setuptools entrypoint
command_suite = (
    # description of the command suite, displayed in cmdline help
    "DataLad FUSE command suite",
    [
        # specification of a command, any number of commands can be defined
        (
            # importable module that contains the command implementation
            "datalad_fuse",
            # name of the command class implementation in above module
            "FuseFS",
            # optional name of the command in the cmdline API
            "fusefs",
            # optional name of the command in the Python API
            "fusefs",
        ),
        ("datalad_fuse.fsspec_head", "FsspecHead", "fsspec-head", "fsspec_head"),
        (
            "datalad_fuse.fsspec_cache_clear",
            "FsspecCacheClear",
            "fsspec-cache-clear",
            "fsspec_cache_clear",
        ),
    ],
)


# decoration auto-generates standard help
@build_doc
# all commands must be derived from Interface
class FuseFS(Interface):
    # first docstring line is used a short description in the cmdline help
    # the rest is put in the verbose help and manpage
    """
    FUSE File system providing transparent access to files under DataLad
    control
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
        "allow_other": Parameter(
            args=("--allow-other",),
            action="store_true",
            doc=(
                "Allow all users to access files in the mount"
                " (Requires configuration in /etc/fuse.conf)"
            ),
        ),
        "foreground": Parameter(
            args=("-f", "--foreground"),
            action="store_true",
            doc="""Run process in foreground [required].""",
        ),
        "mode_transparent": Parameter(
            args=("--mode-transparent",),
            action="store_true",
            doc="Expose .git directory",
        ),
        "caching": Parameter(
            args=("--caching",),
            choices=["none", "ondisk"],
            default="none",
            doc="Whether to cache fsspec'ed files on disk on not at all",
        ),
        # TODO: (might better become config vars?)
        # --cache=persist
        # --recursive=follow,get - encountering submodule might install it first
        # --git=[hide],show - hide .git in the FUSE space to avoid confusion/etc
    }

    @staticmethod
    @datasetmethod(name="fusefs")
    @eval_results
    def __call__(
        mount_path: str,
        dataset: Optional[Dataset] = None,
        foreground: bool = False,
        mode_transparent: bool = False,
        allow_other: bool = False,
        caching: str | None = None,
    ) -> Iterator[Dict[str, Any]]:
        from fuse import FUSE

        from .fuse_ import DataLadFUSE

        if not foreground:
            yield get_status_dict(
                action="fusefs",
                path=mount_path,
                status="error",
                message="fusefs does not work properly without --foreground",
            )
            return
        ds = require_dataset(
            dataset, purpose="mount as FUSE system", check_installed=True
        )
        FUSE(
            DataLadFUSE(
                ds.path,
                mode_transparent=mode_transparent,
                caching=caching == "ondisk",
            ),
            mount_path,
            foreground=foreground,
            allow_other=allow_other,
        )
        yield get_status_dict(action="fusefs", path=mount_path, status="ok")


__version__ = get_versions()["version"]
del get_versions

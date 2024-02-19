from __future__ import annotations

from itertools import islice
import os.path
import sys
from typing import Any, Dict, Iterator, Optional

from datalad.distribution.dataset import (
    Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.interface.base import Interface, build_doc, eval_results
from datalad.interface.results import get_status_dict
from datalad.support.constraints import EnsureInt, EnsureNone, EnsureStr
from datalad.support.param import Parameter

from .fsspec import FsspecAdapter

DEFAULT_LINES = 10


@build_doc
class FsspecHead(Interface):
    """
    Show leading lines/bytes of an annexed file by fetching its data from a
    remote URL
    """

    result_renderer = "tailored"

    _params_ = {
        "dataset": Parameter(
            args=("-d", "--dataset"),
            doc="""dataset to operate on.  If no dataset is given, an
            attempt is made to identify the dataset based on the current
            working directory.""",
            constraints=EnsureDataset() | EnsureNone(),
        ),
        "lines": Parameter(
            args=("-n", "--lines"),
            doc="How many lines to show (default 10)",
            constraints=EnsureInt() | EnsureNone(),
        ),
        "bytes": Parameter(
            args=("-c", "--bytes"),
            doc="How many bytes to show",
            constraints=EnsureInt() | EnsureNone(),
        ),
        "mode_transparent": Parameter(
            args=("--mode-transparent",),
            action="store_true",
            doc="Support reading from .git directory",
        ),
        "caching": Parameter(
            args=("--caching",),
            choices=["none", "ondisk"],
            default="none",
            doc="Whether to cache fsspec'ed files on disk on not at all",
        ),
        "path": Parameter(
            args=("path",),
            doc="Path to an annexed file to show the leading contents of",
            constraints=EnsureStr(),
        ),
    }

    @staticmethod
    @datasetmethod(name="fsspec_head")
    @eval_results
    def __call__(
        path: str,
        dataset: Optional[Dataset] = None,
        lines: Optional[int] = None,
        bytes: Optional[int] = None,
        mode_transparent: bool = False,
        caching: str | None = None,
    ) -> Iterator[Dict[str, Any]]:
        ds = require_dataset(dataset, purpose="fetch file data", check_installed=True)
        if lines is not None and bytes is not None:
            raise ValueError("'lines' and 'bytes' are mutually exclusive")
        elif lines is None and bytes is None:
            lines = DEFAULT_LINES
        with FsspecAdapter(
            ds.path, mode_transparent=mode_transparent, caching=caching == "ondisk"
        ) as fsa:
            if not os.path.isabs(path):
                path = os.path.join(ds.path, path)
            with fsa.open(path) as fp:
                if lines is not None:
                    blob = b"".join(islice(fp, lines))
                else:
                    blob = fp.read(bytes)
        yield get_status_dict(action="fsspec-head", ds=ds, status="ok", data=blob)

    @staticmethod
    def custom_result_renderer(res, **_):
        sys.stdout.buffer.write(res["data"])

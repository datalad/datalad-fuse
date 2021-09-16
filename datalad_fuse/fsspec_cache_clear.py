from typing import Any, Dict, Iterator, Optional

from datalad.distribution.dataset import (
    Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.interface.base import Interface, build_doc
from datalad.interface.results import get_status_dict
from datalad.interface.utils import eval_results
from datalad.support.constraints import EnsureNone
from datalad.support.param import Parameter

from .fsspec import FsspecAdapter


@build_doc
class FsspecCacheClear(Interface):
    """
    Clear fsspec cache
    """

    _params_ = {
        "dataset": Parameter(
            args=("-d", "--dataset"),
            doc="""dataset to operate on.  If no dataset is given, an
            attempt is made to identify the dataset based on the current
            working directory.""",
            constraints=EnsureDataset() | EnsureNone(),
        ),
    }

    @staticmethod
    @datasetmethod(name="fsspec_cache_clear")
    @eval_results
    def __call__(dataset: Optional[Dataset] = None) -> Iterator[Dict[str, Any]]:
        ds = require_dataset(
            dataset, purpose="clear fsspec cache", check_installed=True
        )
        FsspecAdapter(ds.path).clear()
        yield get_status_dict(action="fsspec-cache-clear", ds=ds, status="ok")

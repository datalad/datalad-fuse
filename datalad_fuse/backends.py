"""Abstract base for remote file access backends and shared constants."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import IO, Any, Optional

from .utils import AnnexKey

#: Backends used when neither ``--backends`` nor the
#: ``datalad.fusefs.backends`` configuration option is set.
DEFAULT_BACKENDS = "remfile,fsspec"


class Backend(ABC):
    """Base class for remote file access backends."""

    name: str

    @abstractmethod
    def can_handle(
        self, key: Optional[AnnexKey], mode: str, relpath: Optional[str] = None
    ) -> bool:
        """Return True if this backend should be used for *key* in *mode*.

        *relpath* is the path within the dataset, for backends that dispatch on
        the file name when the annex key carries no suffix (URL/VURL keys).
        """

    @abstractmethod
    def open_url(self, url: str, mode: str = "rb", **kwargs: Any) -> IO:
        """Open *url* and return a file-like object."""

    def clear(self) -> None:  # noqa: B027
        """Clear any caches held by this backend.  Default: no-op."""

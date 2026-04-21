"""Abstract base for remote file access backends and shared constants."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import IO, Any, Optional

from .utils import AnnexKey

DEFAULT_BACKENDS = "remfile,fsspec"


class Backend(ABC):
    """Base class for remote file access backends."""

    name: str

    @abstractmethod
    def can_handle(self, key: Optional[AnnexKey], mode: str) -> bool:
        """Return True if this backend should be used for *key* in *mode*."""

    @abstractmethod
    def open_url(self, url: str, mode: str = "rb", **kwargs: Any) -> IO:
        """Open *url* and return a file-like object."""

    def clear(self) -> None:  # noqa: B027
        """Clear any caches held by this backend.  Default: no-op."""

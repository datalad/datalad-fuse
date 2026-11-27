from __future__ import annotations

import atexit
import os
from pathlib import Path
from typing import Any

import pytest

from datalad_fuse.adapter import _get_shared_adapter, get_shared_adapter


@pytest.mark.ai_generated
def test_get_shared_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    registered: list[tuple[Any, tuple]] = []
    monkeypatch.setattr(
        atexit, "register", lambda func, *args: registered.append((func, args))
    )
    _get_shared_adapter.cache_clear()
    try:
        adapter = get_shared_adapter(caching=False, init_annex=False)
        # The same options, passed differently
        assert get_shared_adapter(False, False, False, None) is adapter
        assert get_shared_adapter(caching=False) is not adapter
        assert adapter.root == Path(os.path.abspath(os.sep))
        assert not adapter.caching
        assert not adapter.init_annex
        assert registered[0] == (adapter.__exit__, (None, None, None))
        assert len(registered) == 2
    finally:
        _get_shared_adapter.cache_clear()

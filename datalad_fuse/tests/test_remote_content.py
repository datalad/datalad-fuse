from __future__ import annotations

from typing import Any

import pytest

from datalad_fuse.adapter import (
    FileState,
    RemoteContent,
    RemoteFilesystemAdapter,
)


@pytest.mark.ai_generated
def test_get_remote_content(url_dataset: Any) -> None:
    ds, data_files = url_dataset
    with RemoteFilesystemAdapter(ds.pathobj, caching=False) as adapter:
        for relpath in data_files:
            path = ds.pathobj / relpath
            state, key = adapter.get_file_state(path)
            content = adapter.get_remote_content(path)
            if state is FileState.NO_CONTENT:
                dsap, _ = adapter.resolve_dataset(path)
                urls = list(dsap.get_urls(str(key)))
                assert urls
                assert content == RemoteContent(key=key, urls=urls)
            else:
                assert content is None
        # A file that is not annexed
        assert adapter.get_remote_content(ds.pathobj / ".datalad" / "config") is None

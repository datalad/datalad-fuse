"""Tests for the pluggable backend system (remfile, fsspec) and adapters."""

from __future__ import annotations

import asyncio
import builtins
from datetime import datetime, timezone
from io import BytesIO
import logging
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Iterator, Optional
from unittest.mock import MagicMock, patch

from fsspec.exceptions import BlocksizeMismatchError
import pytest

from datalad_fuse.adapter import (
    DatasetAdapter,
    FileState,
    RemoteFilesystemAdapter,
    create_backends,
    is_http_url,
    resolve_backends,
)
from datalad_fuse.backends import DEFAULT_BACKENDS, Backend
from datalad_fuse.fsspec import FsspecBackend, on_request_start
from datalad_fuse.remfile import RemfileBackend, RemfileWrapper, _get_remfile
from datalad_fuse.utils import AnnexKey

try:
    import remfile  # noqa: F401

    _has_remfile = True
except ImportError:
    _has_remfile = False

requires_remfile = pytest.mark.skipif(not _has_remfile, reason="remfile not installed")


# -- RemfileBackend.can_handle tests -----------------------------------------


@requires_remfile
class TestRemfileBackendCanHandle:
    """Tests for RemfileBackend extension detection via can_handle."""

    pytestmark = pytest.mark.ai_generated

    @pytest.fixture()
    def remfile_backend(self) -> RemfileBackend:
        return RemfileBackend()

    @pytest.mark.parametrize("ext", sorted(RemfileBackend.EXTENSIONS))
    def test_hdf5_extensions_accepted(
        self, remfile_backend: RemfileBackend, ext: str
    ) -> None:
        key = AnnexKey(backend="MD5E", name="abc123", size=100, suffix=ext)
        assert remfile_backend.can_handle(key, "rb") is True

    @pytest.mark.parametrize("ext", [".txt", ".png", ".csv", ".json", ".zip"])
    def test_non_hdf5_extensions_rejected(
        self, remfile_backend: RemfileBackend, ext: str
    ) -> None:
        key = AnnexKey(backend="MD5E", name="abc123", size=100, suffix=ext)
        assert remfile_backend.can_handle(key, "rb") is False

    def test_no_suffix_rejected(self, remfile_backend: RemfileBackend) -> None:
        key = AnnexKey(backend="MD5", name="abc123", size=100, suffix=None)
        assert remfile_backend.can_handle(key, "rb") is False

    def test_none_key_rejected(self, remfile_backend: RemfileBackend) -> None:
        assert remfile_backend.can_handle(None, "rb") is False

    def test_case_insensitive(self, remfile_backend: RemfileBackend) -> None:
        key = AnnexKey(backend="MD5E", name="abc123", size=100, suffix=".NWB")
        assert remfile_backend.can_handle(key, "rb") is True

    def test_text_mode_rejected(self, remfile_backend: RemfileBackend) -> None:
        key = AnnexKey(backend="MD5E", name="abc123", size=100, suffix=".nwb")
        assert remfile_backend.can_handle(key, "r") is False
        assert remfile_backend.can_handle(key, "rt") is False


class TestFsspecBackendCanHandle:
    """FsspecBackend.can_handle always returns True."""

    @pytest.mark.ai_generated
    def test_always_true(self, tmp_path) -> None:
        backend = FsspecBackend(tmp_path, caching=False)
        key = AnnexKey(backend="MD5E", name="abc123", size=100, suffix=".txt")
        assert backend.can_handle(key, "rb") is True
        assert backend.can_handle(key, "r") is True
        assert backend.can_handle(None, "rb") is True


# -- ABC compliance shared across backends -----------------------------------


def _all_backends(tmp_path) -> list[Backend]:
    out: list[Backend] = [FsspecBackend(tmp_path, caching=False)]
    if _has_remfile:
        out.append(RemfileBackend())
    return out


@pytest.mark.ai_generated
def test_abc_compliance(tmp_path) -> None:
    for backend in _all_backends(tmp_path):
        assert isinstance(backend, Backend)
        assert isinstance(backend.name, str) and backend.name
        assert callable(backend.can_handle)
        assert callable(backend.open_url)
        assert callable(backend.clear)


# -- FsspecBackend retry on BlocksizeMismatchError ---------------------------


@pytest.mark.ai_generated
def test_fsspec_open_retries_on_blocksize_mismatch(tmp_path) -> None:
    """First open() raises BlocksizeMismatchError; cache cleared; retry succeeds."""
    backend = FsspecBackend(tmp_path, caching=False)
    fake_fs = MagicMock()
    good_handle = BytesIO(b"ok")
    fake_fs.open.side_effect = [BlocksizeMismatchError("mismatch"), good_handle]
    backend.fs = fake_fs
    result = backend.open_url("http://example.com/x.bin")
    assert result is good_handle
    fake_fs.pop_from_cache.assert_called_once_with("http://example.com/x.bin")
    assert fake_fs.open.call_count == 2


# -- resolve_backends / create_backends tests --------------------------------


class TestResolveBackends:
    """Verify argument > config > default precedence.

    Note: resolve_backends returns ``(spec, explicit)``.  ``explicit`` is True
    for the argument and config paths, False for the default fallback.
    """

    pytestmark = pytest.mark.ai_generated

    def test_explicit_wins(self) -> None:
        spec, explicit = resolve_backends("fsspec")
        assert spec == "fsspec"
        assert explicit is True

    def test_default(self, monkeypatch) -> None:
        # Ensure config is not consulted; this returns None for any key so
        # resolve_backends falls through to DEFAULT_BACKENDS.
        monkeypatch.setattr(
            "datalad_fuse.adapter.cfg.get",
            lambda _key, default=None: default,
        )
        spec, explicit = resolve_backends(None)
        assert spec == DEFAULT_BACKENDS
        assert explicit is False

    def test_config_override(self, monkeypatch) -> None:
        get = MagicMock(return_value="fsspec")
        monkeypatch.setattr("datalad_fuse.adapter.cfg.get", get)
        spec, explicit = resolve_backends(None)
        assert spec == "fsspec"
        assert explicit is True
        get.assert_called_once_with("datalad.fusefs.backends", None)

    def test_explicit_beats_config(self, monkeypatch) -> None:
        get = MagicMock(return_value="fsspec")
        monkeypatch.setattr("datalad_fuse.adapter.cfg.get", get)
        spec, explicit = resolve_backends("remfile,fsspec")
        assert spec == "remfile,fsspec"
        assert explicit is True
        get.assert_not_called()


class TestCreateBackends:
    pytestmark = pytest.mark.ai_generated

    def test_fsspec_only(self, tmp_path) -> None:
        backends = create_backends("fsspec", tmp_path, caching=False)
        assert len(backends) == 1
        assert backends[0].name == "fsspec"

    def test_unknown_backend_raises(self, tmp_path) -> None:
        with pytest.raises(ValueError, match="Unknown backend"):
            create_backends("nosuch", tmp_path, caching=False)

    def test_unavailable_backend_skipped(self, tmp_path, caplog) -> None:
        with patch("datalad_fuse.remfile._get_remfile", return_value=None):
            backends = create_backends(
                "remfile,fsspec", tmp_path, caching=False, explicit=False
            )
        # remfile skipped, fsspec remains
        assert len(backends) == 1
        assert backends[0].name == "fsspec"
        # No warning when non-explicit
        assert not any(r.levelname == "WARNING" for r in caplog.records)

    def test_unavailable_explicit_backend_warns(self, tmp_path, caplog) -> None:
        import logging

        caplog.set_level(logging.WARNING, logger="datalad.fuse.adapter")
        with patch("datalad_fuse.remfile._get_remfile", return_value=None):
            backends = create_backends(
                "remfile,fsspec", tmp_path, caching=False, explicit=True
            )
        assert len(backends) == 1 and backends[0].name == "fsspec"
        assert any("remfile" in r.message for r in caplog.records)

    def test_all_unavailable_raises(self, tmp_path) -> None:
        with patch("datalad_fuse.remfile._get_remfile", return_value=None):
            with pytest.raises(ValueError, match="No usable backends"):
                create_backends("remfile", tmp_path, caching=False)

    @requires_remfile
    def test_order_preserved(self, tmp_path) -> None:
        backends = create_backends("remfile,fsspec", tmp_path, caching=False)
        assert [b.name for b in backends] == ["remfile", "fsspec"]

    def test_fsspec_with_caching(self, tmp_path) -> None:
        backends = create_backends("fsspec", tmp_path, caching=True)
        assert len(backends) == 1
        assert backends[0].name == "fsspec"
        from fsspec.implementations.cached import CachingFileSystem

        assert isinstance(backends[0].fs, CachingFileSystem)


# -- is_http_url tests -------------------------------------------------------


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "url,expected",
    [
        ("http://example.com/x", True),
        ("https://example.com/x", True),
        ("HTTP://example.com/x", True),
        ("HTTPS://example.com/x", True),
        ("ftp://example.com/x", False),
        ("file:///tmp/x", False),
        ("", False),
        ("/local/path", False),
    ],
)
def test_is_http_url(url: str, expected: bool) -> None:
    assert is_http_url(url) is expected


# -- RemfileWrapper tests ----------------------------------------------------


def _make_mock_remfile(data: bytes = b"hello world\nline two\n") -> MagicMock:
    """Create a mock remfile.File object backed by *data*."""
    mock = MagicMock()
    pos = [0]

    def read(size: int = -1) -> bytes:
        if size == -1:
            result = data[pos[0] :]
            pos[0] = len(data)
            return result
        result = data[pos[0] : pos[0] + size]
        pos[0] = min(pos[0] + size, len(data))
        return result

    def seek(offset: int, whence: int = 0) -> int:
        if whence == 0:
            pos[0] = offset
        elif whence == 1:
            pos[0] += offset
        elif whence == 2:
            pos[0] = len(data) + offset
        return pos[0]

    def tell() -> int:
        return pos[0]

    mock.read = read
    mock.seek = seek
    mock.tell = tell
    mock.close = MagicMock()
    return mock


class TestRemfileWrapper:
    """Tests for the RemfileWrapper adapter class."""

    pytestmark = pytest.mark.ai_generated

    def test_read(self) -> None:
        mock_rf = _make_mock_remfile(b"abcdef")
        w = RemfileWrapper(mock_rf, "http://example.com/test.h5")
        assert w.read(3) == b"abc"
        assert w.read(3) == b"def"

    def test_seek_and_tell(self) -> None:
        mock_rf = _make_mock_remfile(b"abcdef")
        w = RemfileWrapper(mock_rf, "http://example.com/test.h5")
        w.seek(3)
        assert w.tell() == 3
        assert w.read(2) == b"de"

    def test_context_manager(self) -> None:
        mock_rf = _make_mock_remfile()
        w = RemfileWrapper(mock_rf, "http://example.com/test.h5")
        assert not w.closed
        with w as f:
            assert f is w
        assert w.closed
        mock_rf.close.assert_called_once()

    def test_iteration(self) -> None:
        mock_rf = _make_mock_remfile(b"line one\nline two\nline three")
        w = RemfileWrapper(mock_rf, "http://example.com/test.h5")
        assert list(w) == [b"line one\n", b"line two\n", b"line three"]

    def test_iteration_empty(self) -> None:
        mock_rf = _make_mock_remfile(b"")
        w = RemfileWrapper(mock_rf, "http://example.com/test.h5")
        assert list(w) == []

    def test_close(self) -> None:
        mock_rf = _make_mock_remfile()
        w = RemfileWrapper(mock_rf, "http://example.com/test.h5")
        w.close()
        assert w.closed
        mock_rf.close.assert_called_once()

    def test_io_protocol_methods(self) -> None:
        mock_rf = _make_mock_remfile()
        w = RemfileWrapper(mock_rf, "http://example.com/test.h5")
        assert w.readable() is True
        assert w.seekable() is True
        assert w.writable() is False

    def test_info_success(self, monkeypatch) -> None:
        """info() returns {'type': 'file', 'size': N} when Content-Length present."""
        fake_resp = MagicMock()
        fake_resp.headers = {"Content-Length": "1234"}
        fake_resp.__enter__ = MagicMock(return_value=fake_resp)
        fake_resp.__exit__ = MagicMock(return_value=None)
        monkeypatch.setattr("urllib.request.urlopen", MagicMock(return_value=fake_resp))
        w = RemfileWrapper(_make_mock_remfile(), "http://example.com/test.h5")
        assert w.info() == {"type": "file", "size": 1234}

    def test_info_missing_content_length(self, monkeypatch) -> None:
        """info() raises ValueError when Content-Length header absent."""
        fake_resp = MagicMock()
        fake_resp.headers = {}
        fake_resp.__enter__ = MagicMock(return_value=fake_resp)
        fake_resp.__exit__ = MagicMock(return_value=None)
        monkeypatch.setattr("urllib.request.urlopen", MagicMock(return_value=fake_resp))
        w = RemfileWrapper(_make_mock_remfile(), "http://example.com/test.h5")
        with pytest.raises(ValueError, match="Content-Length"):
            w.info()


# -- DatasetAdapter backend-chain and RemoteFilesystemAdapter tests ----------


class _StubBackend(Backend):
    """In-memory backend for exercising DatasetAdapter.open fallback."""

    def __init__(
        self,
        name: str,
        can_handle_result: bool = True,
        open_result: Optional[bytes] = None,
        raises: Optional[Exception] = None,
    ) -> None:
        self.name = name
        self._can = can_handle_result
        self._open_result = open_result
        self._raises = raises
        self.open_calls: list[str] = []
        self.clear_calls = 0

    def can_handle(self, key, mode: str) -> bool:  # noqa: U100
        return self._can

    def open_url(self, url: str, mode: str = "rb", **kwargs):  # noqa: U100
        self.open_calls.append(url)
        if self._raises is not None:
            raise self._raises
        return BytesIO(self._open_result or b"")

    def clear(self) -> None:
        self.clear_calls += 1


def _make_dataset_adapter_with_backends(
    tmp_path, backends: list[Backend], urls: list[str]
) -> DatasetAdapter:
    """Build a DatasetAdapter whose file-state + URL generator are stubbed."""
    # Bypass __init__ (which requires a real datalad dataset on disk)
    adapter = DatasetAdapter.__new__(DatasetAdapter)
    adapter.path = tmp_path
    adapter.mode_transparent = False
    adapter.annex = None
    adapter._backends = backends

    key = AnnexKey(backend="MD5E", name="abc123", size=100, suffix=".nwb")

    def fake_get_file_state(_relpath: str):
        return (FileState.NO_CONTENT, key)

    def fake_get_urls(_key: str) -> Iterator[str]:
        yield from urls

    adapter.get_file_state = fake_get_file_state  # type: ignore[method-assign]
    adapter.get_urls = fake_get_urls  # type: ignore[method-assign]
    return adapter


@pytest.mark.ai_generated
class TestDatasetAdapterOpen:
    """Exercise the backend-chain fallback semantics in DatasetAdapter.open."""

    def test_first_backend_succeeds(self, tmp_path) -> None:
        a = _StubBackend("a", open_result=b"hello")
        b = _StubBackend("b", open_result=b"world")
        adapter = _make_dataset_adapter_with_backends(tmp_path, [a, b], ["http://u1"])
        handle = adapter.open("x.nwb")
        assert handle.read() == b"hello"
        assert a.open_calls == ["http://u1"]
        assert b.open_calls == []

    def test_falls_through_to_next_backend(self, tmp_path) -> None:
        boom = _StubBackend("boom", raises=FileNotFoundError("nope"))
        ok = _StubBackend("ok", open_result=b"good")
        adapter = _make_dataset_adapter_with_backends(
            tmp_path, [boom, ok], ["http://u1"]
        )
        handle = adapter.open("x.nwb")
        assert handle.read() == b"good"
        assert boom.open_calls == ["http://u1"]
        assert ok.open_calls == ["http://u1"]

    def test_skips_backend_that_cannot_handle(self, tmp_path) -> None:
        skipper = _StubBackend(
            "skip", can_handle_result=False, raises=RuntimeError("!")
        )
        ok = _StubBackend("ok", open_result=b"good")
        adapter = _make_dataset_adapter_with_backends(
            tmp_path, [skipper, ok], ["http://u1"]
        )
        handle = adapter.open("x.nwb")
        assert handle.read() == b"good"
        # Skipper must not have been asked to open anything.
        assert skipper.open_calls == []
        assert ok.open_calls == ["http://u1"]

    def test_all_fail_raises_ioerror_with_cause(self, tmp_path) -> None:
        last_exc = FileNotFoundError("last")
        a = _StubBackend("a", raises=FileNotFoundError("first"))
        b = _StubBackend("b", raises=last_exc)
        adapter = _make_dataset_adapter_with_backends(tmp_path, [a, b], ["http://u1"])
        with pytest.raises(IOError) as exc_info:
            adapter.open("x.nwb")
        assert exc_info.value.__cause__ is last_exc
        assert "a,b" in str(exc_info.value)

    def test_non_http_urls_still_tried_per_backend(self, tmp_path) -> None:
        """All provided URLs are tried in order until one succeeds."""
        a = _StubBackend(
            "a",
            raises=FileNotFoundError("no"),
        )
        # With multiple URLs, backend is retried for each.
        adapter = _make_dataset_adapter_with_backends(
            tmp_path, [a], ["http://u1", "http://u2"]
        )
        with pytest.raises(IOError):
            adapter.open("x.nwb")
        assert a.open_calls == ["http://u1", "http://u2"]

    def test_clear_delegates_to_backends(self, tmp_path) -> None:
        a = _StubBackend("a")
        b = _StubBackend("b")
        adapter = _make_dataset_adapter_with_backends(tmp_path, [a, b], [])
        adapter.clear()
        assert a.clear_calls == 1
        assert b.clear_calls == 1

    def test_unsupported_mode_raises(self, tmp_path) -> None:
        adapter = _make_dataset_adapter_with_backends(tmp_path, [_StubBackend("a")], [])
        with pytest.raises(NotImplementedError, match="modes"):
            adapter.open("x.nwb", mode="wb")

    @pytest.mark.parametrize("mode", ["r", "rt"])
    def test_text_mode_forwards_encoding(self, tmp_path, mode) -> None:
        """Mode 'r'/'rt' should pass encoding/errors through to backend.open_url."""
        captured: dict = {}

        class CapturingBackend(Backend):
            name = "cap"

            def can_handle(self, key, mode_):  # noqa: U100
                return True

            def open_url(self, url, mode_="rb", **kwargs):  # noqa: U100
                captured.update(kwargs)
                return BytesIO(b"")

            def clear(self):
                pass

        adapter = _make_dataset_adapter_with_backends(
            tmp_path, [CapturingBackend()], ["http://u1"]
        )
        adapter.open("x.nwb", mode=mode, encoding="latin-1", errors="replace")
        assert captured == {"encoding": "latin-1", "errors": "replace"}

    def test_generic_exception_falls_through(self, tmp_path) -> None:
        """A non-FileNotFoundError (e.g. RuntimeError) should still fall through."""
        boom = _StubBackend("boom", raises=RuntimeError("wat"))
        ok = _StubBackend("ok", open_result=b"fallback")
        adapter = _make_dataset_adapter_with_backends(
            tmp_path, [boom, ok], ["http://u1"]
        )
        handle = adapter.open("x.nwb")
        assert handle.read() == b"fallback"
        assert boom.open_calls == ["http://u1"]
        assert ok.open_calls == ["http://u1"]

    def test_not_annexed_opens_local(self, tmp_path) -> None:
        """NOT_ANNEXED files are opened directly from disk."""
        (tmp_path / "local.txt").write_text("direct read")
        adapter = DatasetAdapter.__new__(DatasetAdapter)
        adapter.path = tmp_path
        adapter.mode_transparent = False
        adapter.annex = None
        adapter._backends = []
        adapter.get_file_state = lambda _r: (  # type: ignore[method-assign]
            FileState.NOT_ANNEXED,
            None,
        )
        with adapter.open("local.txt", mode="r") as f:
            assert f.read() == "direct read"


@pytest.mark.ai_generated
class TestRemoteFilesystemAdapterLifecycle:
    """Context-manager semantics: __exit__ closes datasets and empties the dict."""

    def test_exit_closes_datasets(self, tmp_path) -> None:
        rfs = RemoteFilesystemAdapter(tmp_path, caching=False)
        ds_a = MagicMock()
        ds_b = MagicMock()
        rfs.datasets = {tmp_path / "a": ds_a, tmp_path / "b": ds_b}
        with rfs:
            pass
        ds_a.close.assert_called_once()
        ds_b.close.assert_called_once()
        assert rfs.datasets == {}

    def test_exit_closes_even_on_exception(self, tmp_path) -> None:
        rfs = RemoteFilesystemAdapter(tmp_path, caching=False)
        ds_a = MagicMock()
        rfs.datasets = {tmp_path / "a": ds_a}
        with pytest.raises(RuntimeError):
            with rfs:
                raise RuntimeError("boom")
        ds_a.close.assert_called_once()
        assert rfs.datasets == {}

    def test_get_dataset_path_not_datalad(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setattr("datalad_fuse.adapter.get_dataset_root", lambda _p: None)
        rfs = RemoteFilesystemAdapter(tmp_path, caching=False)
        with pytest.raises(ValueError, match="not under DataLad"):
            rfs.get_dataset_path("anything")

    def test_get_dataset_path_outside_root(self, tmp_path, monkeypatch) -> None:
        other = tmp_path.parent / "elsewhere"
        monkeypatch.setattr(
            "datalad_fuse.adapter.get_dataset_root", lambda _p: str(other)
        )
        rfs = RemoteFilesystemAdapter(tmp_path, caching=False)
        with pytest.raises(ValueError, match="not under root dataset"):
            rfs.get_dataset_path("x")

    def test_delegation_to_dataset_adapter(self, tmp_path) -> None:
        """get_file_state, is_under_annex, get_commit_datetime delegate to DS."""
        rfs = RemoteFilesystemAdapter(tmp_path, caching=False)
        fake_ds = MagicMock()
        dt = datetime(2024, 1, 2, tzinfo=timezone.utc)
        fake_ds.commit_dt = dt
        fake_ds.get_file_state.return_value = (FileState.HAS_CONTENT, None)
        rfs.resolve_dataset = lambda _p: (fake_ds, "rel")  # type: ignore[method-assign]

        assert rfs.get_file_state("x") == (FileState.HAS_CONTENT, None)
        assert rfs.is_under_annex("x") is True
        assert rfs.get_commit_datetime("x") == dt

        fake_ds.get_file_state.return_value = (FileState.NOT_ANNEXED, None)
        assert rfs.is_under_annex("x") is False

    def test_resolve_dataset_reuses_cached_adapter(self, tmp_path, monkeypatch) -> None:
        """resolve_dataset caches DatasetAdapter instances by dataset path."""
        monkeypatch.setattr(
            "datalad_fuse.adapter.get_dataset_root", lambda p: str(Path(p).parent)
        )
        rfs = RemoteFilesystemAdapter(tmp_path, caching=False)
        # Pre-populate so __init__ isn't invoked (no real dataset)
        prebuilt = MagicMock()
        dspath = tmp_path
        rfs.datasets[dspath] = prebuilt
        (tmp_path / "a.txt").write_text("x")
        dsap1, rel1 = rfs.resolve_dataset(tmp_path / "a.txt")
        dsap2, _ = rfs.resolve_dataset(tmp_path / "a.txt")
        assert dsap1 is prebuilt is dsap2
        assert rel1 == "a.txt"


# -- FsspecBackend additional coverage ---------------------------------------


@pytest.mark.ai_generated
def test_fsspec_clear_with_caching(tmp_path) -> None:
    """FsspecBackend.clear() calls fs.clear_cache() when caching=True."""
    backend = FsspecBackend(tmp_path, caching=True)
    backend.fs = MagicMock()
    backend._caching = True
    backend.clear()
    backend.fs.clear_cache.assert_called_once()


@pytest.mark.ai_generated
def test_fsspec_clear_without_caching_noop(tmp_path) -> None:
    """FsspecBackend.clear() is a no-op when caching=False."""
    backend = FsspecBackend(tmp_path, caching=False)
    backend.fs = MagicMock()
    backend.clear()
    backend.fs.clear_cache.assert_not_called()


@pytest.mark.ai_generated
def test_on_request_start_logs_retry(caplog) -> None:
    """on_request_start emits a warning for attempts beyond the first."""
    caplog.set_level(logging.WARNING, logger="datalad.fuse.fsspec")
    ctx = SimpleNamespace(trace_request_ctx={"current_attempt": 2})
    params = SimpleNamespace(url="http://example.com")
    asyncio.run(on_request_start(MagicMock(), ctx, params))
    assert any("Retrying" in r.message for r in caplog.records)

    caplog.clear()
    ctx.trace_request_ctx["current_attempt"] = 1
    asyncio.run(on_request_start(MagicMock(), ctx, params))
    assert not any("Retrying" in r.message for r in caplog.records)


# -- RemfileBackend availability detection -----------------------------------


@pytest.mark.ai_generated
def test_remfile_backend_raises_when_unavailable(monkeypatch) -> None:
    """RemfileBackend.__init__ raises ImportError when remfile is not installed."""
    monkeypatch.setattr("datalad_fuse.remfile._get_remfile", lambda: None)
    with pytest.raises(ImportError, match="remfile"):
        RemfileBackend()


@pytest.mark.ai_generated
def test_get_remfile_returns_none_on_import_error(monkeypatch) -> None:
    """_get_remfile() swallows ImportError and returns None."""
    # Block remfile from being imported: shadow sys.modules so the
    # next `import remfile` fails, and shadow __import__ to guarantee
    # ImportError is raised even if remfile was cached.
    monkeypatch.setitem(sys.modules, "remfile", None)
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "remfile":
            raise ImportError("not here")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert _get_remfile() is None


# -- Integration tests with real S3 URLs -------------------------------------

# Pinned versions from dandiarchive S3 bucket
S3_HDF5_URL = (
    "https://dandiarchive.s3.amazonaws.com/ros3test.hdf5"
    "?versionId=_8Zs6qF7E6vpc5BcPOhizlFBY2oHCN8T"
)
S3_NWB_URL = (
    "https://dandiarchive.s3.amazonaws.com/ros3test.nwb"
    "?versionId=jRN4ejAcjOAaFDXTQrO99WCZqTxhZL32"
)
S3_README_URL = (
    "https://dandiarchive.s3.amazonaws.com/README.md"
    "?versionId=mAywZ4KP9BCgGIERb3DtlPzTWYv.5sUi"
)


@requires_remfile
class TestIntegrationRemfileBackend:
    """Integration tests using real S3 URLs via the RemfileBackend."""

    pytestmark = [pytest.mark.ai_generated, pytest.mark.network]

    @pytest.fixture()
    def remfile_backend(self) -> RemfileBackend:
        return RemfileBackend()

    def test_open_hdf5(self, remfile_backend: RemfileBackend) -> None:
        with remfile_backend.open_url(S3_HDF5_URL) as f:
            assert f.read(8) == b"\x89HDF\r\n\x1a\n"

    def test_open_nwb(self, remfile_backend: RemfileBackend) -> None:
        with remfile_backend.open_url(S3_NWB_URL) as f:
            assert f.read(8) == b"\x89HDF\r\n\x1a\n"

    def test_seek_and_reread(self, remfile_backend: RemfileBackend) -> None:
        with remfile_backend.open_url(S3_HDF5_URL) as f:
            first = f.read(8)
            f.seek(0)
            second = f.read(8)
            assert first == second == b"\x89HDF\r\n\x1a\n"

    def test_wrapper_context_manager(self, remfile_backend: RemfileBackend) -> None:
        f = remfile_backend.open_url(S3_HDF5_URL)
        with f:
            assert f.read(4) == b"\x89HDF"
        assert f.closed


class TestIntegrationFsspecBackend:
    """Integration tests using real S3 URLs via the FsspecBackend."""

    pytestmark = [pytest.mark.ai_generated, pytest.mark.network]

    @pytest.fixture()
    def fsspec_backend(self, tmp_path) -> FsspecBackend:
        return FsspecBackend(tmp_path, caching=False)

    def test_open_readme(self, fsspec_backend: FsspecBackend) -> None:
        with fsspec_backend.open_url(S3_README_URL) as f:
            data = f.read(100)
            assert len(data) > 0
            assert isinstance(data, bytes)

    def test_open_hdf5(self, fsspec_backend: FsspecBackend) -> None:
        with fsspec_backend.open_url(S3_HDF5_URL) as f:
            assert f.read(8) == b"\x89HDF\r\n\x1a\n"


@requires_remfile
class TestIntegrationBackendChain:
    """Test backend chain selection logic."""

    pytestmark = pytest.mark.ai_generated

    def test_remfile_selected_for_hdf5(self, tmp_path) -> None:
        backends = create_backends("remfile,fsspec", tmp_path, caching=False)
        key = AnnexKey(backend="MD5E", name="abc", size=100, suffix=".hdf5")
        for b in backends:
            if b.can_handle(key, "rb"):
                assert b.name == "remfile"
                break

    def test_fsspec_selected_for_md(self, tmp_path) -> None:
        backends = create_backends("remfile,fsspec", tmp_path, caching=False)
        key = AnnexKey(backend="MD5E", name="abc", size=100, suffix=".md")
        for b in backends:
            if b.can_handle(key, "rb"):
                assert b.name == "fsspec"
                break

    def test_fsspec_only_chain(self, tmp_path) -> None:
        backends = create_backends("fsspec", tmp_path, caching=False)
        key = AnnexKey(backend="MD5E", name="abc", size=100, suffix=".nwb")
        for b in backends:
            if b.can_handle(key, "rb"):
                assert b.name == "fsspec"
                break

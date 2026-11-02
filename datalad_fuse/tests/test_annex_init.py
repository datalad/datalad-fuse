from __future__ import annotations

from pathlib import Path
import subprocess

from datalad.api import Dataset
import pytest

from datalad_fuse.fsspec import DatasetAdapter, FsspecAdapter


@pytest.fixture
def annex_clone(tmp_path: Path) -> Path:
    """A ``git clone`` (with git-annex not initialized) of a dataset with an
    annexed file, whose content is thus missing"""
    ds = Dataset(tmp_path / "ds").create()
    (ds.pathobj / "data.bin").write_bytes(b"0123456789" * 1000)
    ds.save(message="Add data")
    clone = tmp_path / "clone"
    subprocess.run(["git", "clone", "-q", str(ds.path), str(clone)], check=True)
    return clone


def annex_uuid(path: Path) -> str:
    r = subprocess.run(
        ["git", "-C", str(path), "config", "--get", "annex.uuid"],
        capture_output=True,
        text=True,
    )
    return r.stdout.strip()


@pytest.mark.usefixtures("tmp_home")
def test_adapter_no_init_annex(annex_clone: Path) -> None:
    dsap = DatasetAdapter(annex_clone, caching=False, init_annex=False)
    try:
        assert not dsap.annex_initialized
        assert dsap.annex_disabled
        state, key = dsap.get_file_state("data.bin")
        assert state.name == "NO_CONTENT"
        assert key is not None and key.size == 10_000
        with pytest.raises(IOError, match="not initialized"):
            dsap.open("data.bin")
    finally:
        dsap.close()
    assert annex_uuid(annex_clone) == ""


@pytest.mark.usefixtures("tmp_home")
def test_adapter_passes_init_annex(annex_clone: Path) -> None:
    with FsspecAdapter(annex_clone, caching=False, init_annex=False) as adapter:
        dsap, relpath = adapter.resolve_dataset(annex_clone / "data.bin")
        assert relpath == "data.bin"
        assert dsap.annex_disabled
        with pytest.raises(IOError, match="not initialized"):
            adapter.open(annex_clone / "data.bin")
    assert annex_uuid(annex_clone) == ""

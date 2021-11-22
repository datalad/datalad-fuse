import os.path
import subprocess

import pytest


@pytest.mark.libfuse
def test_fuse(tmp_path, url_dataset):
    ds, data_files = url_dataset
    p = subprocess.Popen(
        ["datalad", "fusefs", "-d", ds.path, "--foreground", str(tmp_path)]
    )
    # Check that the command didn't fail immediately:
    with pytest.raises(subprocess.TimeoutExpired):
        p.wait(timeout=3)
    assert (
        sorted(q.name for q in tmp_path.iterdir())
        == [
            ".datalad",
            ".gitattributes",
        ]
        + sorted(data_files)
    )
    for fname, blob in data_files.items():
        assert (tmp_path / fname).read_bytes() == blob
    p.terminate()


@pytest.mark.libfuse
def test_fuse_subdataset(tmp_path, superdataset):
    ds, data_files = superdataset
    p = subprocess.Popen(
        ["datalad", "fusefs", "-d", ds.path, "--foreground", str(tmp_path)]
    )
    # Check that the command didn't fail immediately:
    with pytest.raises(subprocess.TimeoutExpired):
        p.wait(timeout=3)
    assert sorted(q.name for q in tmp_path.iterdir()) == [
        ".datalad",
        ".gitattributes",
        ".gitmodules",
        "sub",
    ]
    assert (
        sorted(q.name for q in (tmp_path / "sub").iterdir())
        == [
            ".datalad",
            ".gitattributes",
        ]
        + sorted(os.path.relpath(fname, "sub") for fname in data_files)
    )
    for fname, blob in data_files.items():
        assert (tmp_path / fname).read_bytes() == blob
    p.terminate()

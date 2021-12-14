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
@pytest.mark.parametrize("cache_clear", [None, "recursive", "visited"])
def test_fuse_subdataset(tmp_path, superdataset, cache_clear, tmp_home):
    if cache_clear is not None:
        with (tmp_home / ".gitconfig").open("a") as fp:
            print("", file=fp)
            print('[datalad "fusefs"]', file=fp)
            print(f"cache-clear = {cache_clear}", file=fp)
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
    p.wait()
    cachedir = ds.pathobj / "sub" / ".git" / "datalad" / "cache" / "fsspec"
    is_local = (ds.pathobj / next(iter(data_files))).exists()
    if is_local and cache_clear != "recursive":
        assert not cachedir.exists()
    elif cache_clear is not None:
        assert list(cachedir.iterdir()) == []
    else:
        assert list(cachedir.iterdir()) != []

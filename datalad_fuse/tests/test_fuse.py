import os.path
import subprocess

from datalad.api import Dataset
import pytest

pytestmark = pytest.mark.libfuse


@pytest.mark.parametrize("transparent", [False, True])
def test_fuse(tmp_path, transparent, url_dataset):
    ds, data_files = url_dataset
    if transparent:
        opts = ["--mode-transparent"]
        dots = [".datalad", ".git", ".gitattributes"]
    else:
        opts = []
        dots = [".datalad", ".gitattributes"]
    p = subprocess.Popen(
        ["datalad", "fusefs", "-d", ds.path, "--foreground", str(tmp_path), *opts]
    )
    # Check that the command didn't fail immediately:
    with pytest.raises(subprocess.TimeoutExpired):
        p.wait(timeout=3)
    assert sorted(q.name for q in tmp_path.iterdir()) == dots + sorted(data_files)
    for fname, blob in data_files.items():
        assert (tmp_path / fname).read_bytes() == blob
    p.terminate()


@pytest.mark.parametrize("cache_clear", [None, "recursive", "visited"])
@pytest.mark.parametrize("transparent", [False, True])
def test_fuse_subdataset(tmp_path, superdataset, cache_clear, transparent, tmp_home):
    if cache_clear is not None:
        with (tmp_home / ".gitconfig").open("a") as fp:
            print("", file=fp)
            print('[datalad "fusefs"]', file=fp)
            print(f"cache-clear = {cache_clear}", file=fp)
    ds, data_files = superdataset
    if transparent:
        opts = ["--mode-transparent"]
        dots = [".datalad", ".git", ".gitattributes"]
    else:
        opts = []
        dots = [".datalad", ".gitattributes"]
    p = subprocess.Popen(
        ["datalad", "fusefs", "-d", ds.path, "--foreground", str(tmp_path), *opts]
    )
    # Check that the command didn't fail immediately:
    with pytest.raises(subprocess.TimeoutExpired):
        p.wait(timeout=3)
    assert sorted(q.name for q in tmp_path.iterdir()) == dots + [".gitmodules", "sub"]
    assert sorted(q.name for q in (tmp_path / "sub").iterdir()) == dots + sorted(
        os.path.relpath(fname, "sub") for fname in data_files
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


def test_fuse_transparent_hash_object(tmp_path):
    ds = Dataset(tmp_path / "ds").create()
    mount = tmp_path / "mount"
    mount.mkdir()
    p = subprocess.Popen(
        [
            "datalad",
            "fusefs",
            "-d",
            ds.path,
            "--foreground",
            "--mode-transparent",
            str(mount),
        ]
    )
    with pytest.raises(subprocess.TimeoutExpired):
        p.wait(timeout=3)
    CONTENT = "This is test text.\n"
    r = subprocess.run(
        ["git", "-P", "--git-dir", str(mount / ".git"), "hash-object", "-w", "--stdin"],
        cwd=mount,
        check=True,
        universal_newlines=True,
        input=CONTENT,
        stdout=subprocess.PIPE,
    )
    blobhash = r.stdout.strip()
    r = subprocess.run(
        ["git", "-P", "--git-dir", str(mount / ".git"), "hash-object", "-w", "--stdin"],
        cwd=mount,
        check=True,
        universal_newlines=True,
        input=CONTENT,
        stdout=subprocess.PIPE,
    )
    assert r.stdout.strip() == blobhash
    r = subprocess.run(
        ["git", "-P", "--git-dir", str(mount / ".git"), "show", blobhash],
        cwd=mount,
        check=True,
        universal_newlines=True,
        stdout=subprocess.PIPE,
    )
    assert r.stdout == CONTENT
    p.terminate()
    r = subprocess.run(
        ["git", "-P", "show", blobhash],
        cwd=ds.path,
        check=True,
        universal_newlines=True,
        stdout=subprocess.PIPE,
    )
    assert r.stdout == CONTENT


def test_fuse_transparent_hash_object_subdataset(tmp_path):
    ds = Dataset(tmp_path / "ds").create()
    ds.create(tmp_path / "ds" / "sub")
    mount = tmp_path / "mount"
    mount.mkdir()
    p = subprocess.Popen(
        [
            "datalad",
            "fusefs",
            "-d",
            ds.path,
            "--foreground",
            "--mode-transparent",
            str(mount),
        ]
    )
    with pytest.raises(subprocess.TimeoutExpired):
        p.wait(timeout=3)
    CONTENT = "This is test text.\n"
    r = subprocess.run(
        [
            "git",
            "-P",
            "--git-dir",
            str(mount / "sub" / ".git"),
            "hash-object",
            "-w",
            "--stdin",
        ],
        cwd=mount / "sub",
        check=True,
        universal_newlines=True,
        input=CONTENT,
        stdout=subprocess.PIPE,
    )
    blobhash = r.stdout.strip()
    r = subprocess.run(
        ["git", "-P", "--git-dir", str(mount / ".git"), "hash-object", "-w", "--stdin"],
        cwd=mount,
        check=True,
        universal_newlines=True,
        input=CONTENT,
        stdout=subprocess.PIPE,
    )
    assert r.stdout.strip() == blobhash
    r = subprocess.run(
        ["git", "-P", "--git-dir", str(mount / "sub" / ".git"), "show", blobhash],
        cwd=mount / "sub",
        check=True,
        universal_newlines=True,
        stdout=subprocess.PIPE,
    )
    assert r.stdout == CONTENT
    p.terminate()
    r = subprocess.run(
        ["git", "-P", "show", blobhash],
        cwd=ds.pathobj / "sub",
        check=True,
        universal_newlines=True,
        stdout=subprocess.PIPE,
    )
    assert r.stdout == CONTENT


def test_fuse_lock(tmp_path):
    CONTENT = "This is test text.\n"
    ds = Dataset(tmp_path / "ds").create(cfg_proc="text2git")
    (tmp_path / "ds" / "text.txt").write_text(CONTENT)
    ds.save(message="Create text file")
    mount = tmp_path / "mount"
    mount.mkdir()
    p = subprocess.Popen(
        [
            "datalad",
            "fusefs",
            "-d",
            ds.path,
            "--foreground",
            "--mode-transparent",
            str(mount),
        ]
    )
    with pytest.raises(subprocess.TimeoutExpired):
        p.wait(timeout=3)
    try:
        subprocess.run(
            ["git-annex", "smudge", "--clean", "--", "text.txt"],
            cwd=mount,
            check=True,
            input=CONTENT,
            universal_newlines=True,
        )
    finally:
        p.terminate()

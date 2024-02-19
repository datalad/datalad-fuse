from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import hashlib
import os.path
from pathlib import Path
import subprocess
from typing import Iterator, Union

from datalad.api import Dataset
import pytest

pytestmark = pytest.mark.libfuse


@contextmanager
def fusing(
    source_dir: Union[str, Path],
    mount_dir: Path,
    transparent: bool = False,
    wait: bool = False,
    caching: bool = False,
) -> Iterator[Path]:
    mount_dir.mkdir(parents=True, exist_ok=True)
    opts = []
    if transparent:
        opts.append("--mode-transparent")
    if caching:
        opts.append("--caching=ondisk")
    p = subprocess.Popen(
        [
            "datalad",
            "fusefs",
            "-d",
            str(source_dir),
            "--foreground",
            *opts,
            str(mount_dir),
        ]
    )
    # Check that the command didn't fail immediately:
    with pytest.raises(subprocess.TimeoutExpired):
        p.wait(timeout=3)
    try:
        yield mount_dir
    finally:
        p.terminate()
        p.wait(timeout=3)
    if wait:
        p.wait()


@pytest.mark.parametrize("transparent", [False, True])
@pytest.mark.parametrize("caching", [False, True])
def test_fuse(tmp_path, transparent, caching, url_dataset):
    ds, data_files = url_dataset
    if transparent:
        dots = [".datalad", ".git", ".gitattributes"]
    else:
        dots = [".datalad", ".gitattributes"]
    with fusing(ds.path, tmp_path, transparent=transparent, caching=caching) as mount:
        assert sorted(q.name for q in mount.iterdir()) == dots + sorted(data_files)
        for fname, blob in data_files.items():
            assert os.path.getsize(mount / fname) == len(blob)
            assert (mount / fname).read_bytes() == blob


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
        dots = [".datalad", ".git", ".gitattributes"]
    else:
        dots = [".datalad", ".gitattributes"]
    with fusing(
        ds.path, tmp_path, caching=True, transparent=transparent, wait=True
    ) as mount:
        assert sorted(q.name for q in mount.iterdir()) == dots + [".gitmodules", "sub"]
        assert sorted(q.name for q in (mount / "sub").iterdir()) == dots + sorted(
            os.path.relpath(fname, "sub") for fname in data_files
        )
        for fname, blob in data_files.items():
            assert (mount / fname).read_bytes() == blob
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
    with fusing(ds.path, tmp_path / "mount", transparent=True) as mount:
        CONTENT = "This is test text.\n"
        r = subprocess.run(
            [
                "git",
                "-P",
                "--git-dir",
                str(mount / ".git"),
                "hash-object",
                "-w",
                "--stdin",
            ],
            cwd=mount,
            check=True,
            universal_newlines=True,
            input=CONTENT,
            stdout=subprocess.PIPE,
        )
        blobhash = r.stdout.strip()
        r = subprocess.run(
            [
                "git",
                "-P",
                "--git-dir",
                str(mount / ".git"),
                "hash-object",
                "-w",
                "--stdin",
            ],
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
    with fusing(ds.path, tmp_path / "mount", transparent=True) as mount:
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
            [
                "git",
                "-P",
                "--git-dir",
                str(mount / ".git"),
                "hash-object",
                "-w",
                "--stdin",
            ],
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
    with fusing(ds.path, tmp_path / "mount", transparent=True) as mount:
        subprocess.run(
            ["git-annex", "smudge", "--clean", "--", "text.txt"],
            cwd=mount,
            check=True,
            input=CONTENT,
            universal_newlines=True,
        )


def test_fuse_git_status(tmp_path):
    CONTENT = "This is test text.\n"
    ds = Dataset(tmp_path / "ds").create(cfg_proc="text2git")
    (tmp_path / "ds" / "text.txt").write_text(CONTENT)
    ds.save(message="Create text file")
    with fusing(ds.path, tmp_path / "mount", transparent=True) as mount:
        r = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=mount,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )
        assert r.stdout == ""


def test_parallel_access(tmp_path, big_url_dataset):
    ds, data_files = big_url_dataset
    with fusing(ds.path, tmp_path) as mount:
        with ThreadPoolExecutor() as pool:
            futures = {
                pool.submit(sha256_file, mount / path): dgst
                for path, dgst in data_files.items()
            }
            for fut in as_completed(futures.keys()):
                assert fut.result() == futures[fut]


def sha256_file(path):
    dgst = hashlib.sha256()
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(65535), b""):
            dgst.update(chunk)
    return dgst.hexdigest()

from pathlib import Path
import subprocess

from datalad.api import Dataset
from datalad.tests.utils_pytest import assert_in_results
from linesep import split_terminated
import pytest


def first_n_lines(blob, n):
    # In order to match the behavior of Python's binary IO files, only \n
    # should be treated as a line separator, so bytes.splitlines() is not an
    # option.
    return b"".join(split_terminated(blob, b"\n", True)[:n])


def test_get_default_lines_text(url_dataset):
    ds, data_files = url_dataset
    assert_in_results(
        ds.fsspec_head("text.txt"),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(data_files["text.txt"], 10),
    )


@pytest.mark.parametrize("lines", [0, 4, 20])
def test_get_lines_text(lines, url_dataset):
    ds, data_files = url_dataset
    assert_in_results(
        ds.fsspec_head("text.txt", lines=lines),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(data_files["text.txt"], lines),
    )


def test_get_bytes_text(url_dataset):
    ds, data_files = url_dataset
    assert_in_results(
        ds.fsspec_head("text.txt", bytes=100),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=data_files["text.txt"][:100],
    )


def test_get_lines_binary(url_dataset):
    ds, data_files = url_dataset
    assert_in_results(
        ds.fsspec_head("binary.png", lines=3),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(data_files["binary.png"], 3),
    )


def test_get_bytes_binary(url_dataset):
    ds, data_files = url_dataset
    assert_in_results(
        ds.fsspec_head("binary.png", bytes=100),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=data_files["binary.png"][:100],
    )


def test_subdataset_get_default_lines_text(superdataset):
    ds, data_files = superdataset
    assert_in_results(
        ds.fsspec_head("sub/text.txt"),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(data_files["sub/text.txt"], 10),
    )


@pytest.mark.parametrize("lines", [0, 4, 20])
def test_subdataset_get_lines_text(lines, superdataset):
    ds, data_files = superdataset
    assert_in_results(
        ds.fsspec_head("sub/text.txt", lines=lines),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(data_files["sub/text.txt"], lines),
    )


def test_subdataset_get_bytes_text(superdataset):
    ds, data_files = superdataset
    assert_in_results(
        ds.fsspec_head("sub/text.txt", bytes=100),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=data_files["sub/text.txt"][:100],
    )


def test_subdataset_get_lines_binary(superdataset):
    ds, data_files = superdataset
    assert_in_results(
        ds.fsspec_head("sub/binary.png", lines=3),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(data_files["sub/binary.png"], 3),
    )


def test_subdataset_get_bytes_binary(superdataset):
    ds, data_files = superdataset
    assert_in_results(
        ds.fsspec_head("sub/binary.png", bytes=100),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=data_files["sub/binary.png"][:100],
    )


def test_git_repo(tmp_path):
    subprocess.run(["git", "init"], cwd=tmp_path, check=True)
    TEXT = (Path(__file__).with_name("data") / "text.txt").read_bytes()
    (tmp_path / "text.txt").write_bytes(TEXT)
    subprocess.run(["git", "add", "text.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "Add a file"], cwd=tmp_path, check=True)
    ds = Dataset(tmp_path)
    assert_in_results(
        ds.fsspec_head("text.txt"),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(TEXT, 10),
    )

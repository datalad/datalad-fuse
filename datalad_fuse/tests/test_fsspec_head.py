from datalad.tests.utils import assert_in_results
from linesep import split_terminated
import pytest


def first_n_lines(blob, n):
    # In order to match the behavior of Python's binary IO files, only \n
    # should be treated as a line separator, so bytes.splitlines() is not an
    # option.
    return b"".join(split_terminated(blob, b"\n", True)[:n])


def test_get_default_lines_text(data_files, url_dataset):
    assert_in_results(
        url_dataset.fsspec_head("text.txt"),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(data_files["text.txt"], 10),
    )


@pytest.mark.parametrize("lines", [0, 4, 20])
def test_get_lines_text(data_files, lines, url_dataset):
    assert_in_results(
        url_dataset.fsspec_head("text.txt", lines=lines),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(data_files["text.txt"], lines),
    )


def test_get_bytes_text(data_files, url_dataset):
    assert_in_results(
        url_dataset.fsspec_head("text.txt", bytes=100),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=data_files["text.txt"][:100],
    )


def test_get_lines_binary(data_files, url_dataset):
    assert_in_results(
        url_dataset.fsspec_head("binary.png", lines=3),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=first_n_lines(data_files["binary.png"], 3),
    )


def test_get_bytes_binary(data_files, url_dataset):
    assert_in_results(
        url_dataset.fsspec_head("binary.png", bytes=100),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=data_files["binary.png"][:100],
    )

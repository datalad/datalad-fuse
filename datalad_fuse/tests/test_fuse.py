import subprocess

from datalad.tests.utils import assert_in_results
import pytest


@pytest.mark.libfuse
def test_fuse(data_files, tmp_path, url_dataset):
    assert_in_results(
        url_dataset.fusefs(str(tmp_path)),
        action="fusefs",
        type="dataset",
        path=str(tmp_path),
        status="ok",
    )
    for fname, blob in data_files.items():
        assert (tmp_path / fname).read_bytes() == blob
    subprocess.run(["fusermount", "-u", str(tmp_path)], check=True)

import subprocess

import pytest


@pytest.mark.libfuse
def test_fuse(data_files, tmp_path, url_dataset):
    p = subprocess.Popen(
        ["datalad", "fusefs", "-d", url_dataset.path, "--foreground", str(tmp_path)]
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

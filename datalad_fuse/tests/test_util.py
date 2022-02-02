from typing import Optional, Tuple

import pytest

from datalad_fuse.fsspec import filename2key
from datalad_fuse.utils import is_annex_dir_or_key

SAMPLE_KEY = "MD5E-s1064--8804d3d11f17e33bd912f1f0947afdb9.json"
URL_KEY = "URL--http&c%%127.0.0.1&c55485%binary.png"


@pytest.mark.parametrize(
    "path,expected",
    [
        (f".git/annex/objects/p0/4v/{SAMPLE_KEY}/{SAMPLE_KEY}", (".", "key")),
        (f".git/annex/objects/p2/pX/{URL_KEY}/{URL_KEY}", (".", "key")),
        (f".git/annex/objects/p0/4v/{SAMPLE_KEY}/", (".", "dir")),
        (f".git/annex/objects/p0/4v/{SAMPLE_KEY}", (".", "dir")),
        (".git/annex/objects/p0/4v", (".", "dir")),
        (
            f"some/project/.git/annex/objects/p0/4v/{SAMPLE_KEY}/{SAMPLE_KEY}",
            ("some/project", "key"),
        ),
        ("some/project/.git/annex/objects/p0/4v", ("some/project", "dir")),
        (
            f"/usr/src/project/.git/annex/objects/p0/4v/{SAMPLE_KEY}/{SAMPLE_KEY}",
            ("/usr/src/project", "key"),
        ),
        ("/usr/src/project/.git/annex/objects/p0/4v", ("/usr/src/project", "dir")),
        ("foo.txt", None),
        ("foo.git/annex/objects/p0/4v", None),
        ("some/project/.git/refs/heads", None),
        ("some/project/.git/annex", None),
        ("some/project/.git/annex/other", None),
        ("some/project/.git/annex/objects/layout_config.json", None),
        ("some/project/.git/annex/objects/p0/layout_config.json", None),
        ("some/project/.git/annex/objects/p0/4v/layout_config.json", None),
        (f"some/project/.git/annex/objects/p0/4v/{SAMPLE_KEY}/notmatchingkey", None),
        (
            f"some/project/.git/annex/objects/p0/4v/{SAMPLE_KEY}/{SAMPLE_KEY}/notmatchingkey",
            None,
        ),
        ("some/project/.git/annex/objects/p0/4v/notmatchingkey/notmatchingkey", None),
        (
            "some/project/.git/embedded/sub/.git/annex/objects/p0/4v/"
            f"{SAMPLE_KEY}/{SAMPLE_KEY}",
            ("some/project/.git/embedded/sub", "key"),
        ),
        (
            "some/project/.git/embedded/sub/.git/annex/objects/p0/4v",
            ("some/project/.git/embedded/sub", "dir"),
        ),
    ],
)
def test_is_annex_dir_or_key(path: str, expected: Optional[Tuple[str, str]]) -> None:
    assert is_annex_dir_or_key(path) == expected


@pytest.mark.parametrize(
    "filename,key",
    [
        (
            "URL--http&c%%127.0.0.1&c35401%text.txt",
            "URL--http://127.0.0.1:35401/text.txt",
        ),
        ("foo&ac", "foo&c"),
        ("foo&a&s", "foo&%"),
    ],
)
def test_filename2key(filename: str, key: str) -> None:
    assert filename2key(filename) == key

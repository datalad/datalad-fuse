from __future__ import annotations

import pytest

from datalad_fuse.utils import AnnexDir, AnnexKey, is_annex_dir_or_key

SAMPLE_KEY = "MD5E-s1064--8804d3d11f17e33bd912f1f0947afdb9.json"
URL_KEY = "URL--http&c%%127.0.0.1&c55485%binary.png"

SAMPLE_ANNEX_KEY = AnnexKey(
    backend="MD5E",
    size=1064,
    name="8804d3d11f17e33bd912f1f0947afdb9",
    suffix=".json",
)

URL_ANNEX_KEY = AnnexKey(backend="URL", name="http://127.0.0.1:55485/binary.png")


@pytest.mark.parametrize(
    "path,expected",
    [
        (f".git/annex/objects/p0/4v/{SAMPLE_KEY}/{SAMPLE_KEY}", SAMPLE_ANNEX_KEY),
        (f".git/annex/objects/p2/pX/{URL_KEY}/{URL_KEY}", URL_ANNEX_KEY),
        (f".git/annex/objects/p0/4v/{SAMPLE_KEY}/", AnnexDir(".")),
        (f".git/annex/objects/p0/4v/{SAMPLE_KEY}", AnnexDir(".")),
        (".git/annex/objects/p0/4v", AnnexDir(".")),
        (
            f"some/project/.git/annex/objects/p0/4v/{SAMPLE_KEY}/{SAMPLE_KEY}",
            SAMPLE_ANNEX_KEY,
        ),
        ("some/project/.git/annex/objects/p0/4v", AnnexDir("some/project")),
        (
            f"/usr/src/project/.git/annex/objects/p0/4v/{SAMPLE_KEY}/{SAMPLE_KEY}",
            SAMPLE_ANNEX_KEY,
        ),
        ("/usr/src/project/.git/annex/objects/p0/4v", AnnexDir("/usr/src/project")),
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
            SAMPLE_ANNEX_KEY,
        ),
        (
            "some/project/.git/embedded/sub/.git/annex/objects/p0/4v",
            AnnexDir("some/project/.git/embedded/sub"),
        ),
    ],
)
def test_is_annex_dir_or_key(path: str, expected: AnnexDir | AnnexKey | None) -> None:
    assert is_annex_dir_or_key(path) == expected

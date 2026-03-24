"""Integration tests for Forgejo-aneksajo support.

These tests require a container runtime (podman or docker) and will
auto-skip if none is available.  See CONTRIBUTING.md for details on
controlling the test container via environment variables.
"""

from __future__ import annotations

import subprocess

import pytest
import requests

from datalad_fuse.fsspec import DatasetAdapter, _is_aneksajo

from .conftest_forgejo import ForgejoInstance, ForgejoRepo

pytestmark = pytest.mark.network


# -- API detection -----------------------------------------------------------


@pytest.mark.ai_generated
def test_forgejo_api_version_detection(
    forgejo_instance: ForgejoInstance,
) -> None:
    """/api/forgejo/v1/version returns a version containing 'git-annex'."""
    resp = requests.get(
        f"{forgejo_instance.url}/api/forgejo/v1/version",
        timeout=10,
    )
    assert resp.status_code == 200
    assert "git-annex" in resp.json()["version"]


@pytest.mark.ai_generated
def test_is_aneksajo_detection(forgejo_instance: ForgejoInstance) -> None:
    """_is_aneksajo() returns True for a Forgejo-aneksajo instance."""
    assert _is_aneksajo(forgejo_instance.url) is True


@pytest.mark.ai_generated
def test_is_aneksajo_negative() -> None:
    """_is_aneksajo() returns False for an unreachable host."""
    assert _is_aneksajo("http://127.0.0.1:1") is False


# -- annex/objects URL accessibility -----------------------------------------


def _annex_objects_url(repo: ForgejoRepo) -> str:
    """Build the ``/{owner}/{repo}/annex/objects/…`` URL for *repo*."""
    annex_path = subprocess.run(
        [
            "git",
            "annex",
            "examinekey",
            "--format=annex/objects/${hashdirlower}${key}/${key}",
            repo.annex_key,
        ],
        capture_output=True,
        text=True,
        check=True,
        cwd=repo.local_path,
    ).stdout.strip()

    base = repo.remote_url.rstrip("/")
    if base.endswith(".git"):
        base = base[:-4].rstrip("/")
    return f"{base}/{annex_path}"


@pytest.mark.ai_generated
def test_forgejo_annex_objects_url_accessible(
    forgejo_repo: ForgejoRepo,
) -> None:
    """The annex/objects endpoint serves content and supports Range."""
    url = _annex_objects_url(forgejo_repo)

    # HEAD — 200, Content-Length, Accept-Ranges
    head = requests.head(url, timeout=10)
    assert head.status_code == 200
    assert head.headers.get("accept-ranges") == "bytes"
    assert int(head.headers["content-length"]) == len(forgejo_repo.content)

    # Range — 206 partial
    partial = requests.get(url, headers={"Range": "bytes=0-9"}, timeout=10)
    assert partial.status_code == 206
    assert partial.content == forgejo_repo.content[:10]

    # Full GET
    full = requests.get(url, timeout=10)
    assert full.status_code == 200
    assert full.content == forgejo_repo.content


# -- DatasetAdapter integration ----------------------------------------------


@pytest.mark.ai_generated
def test_get_urls_generates_forgejo_url(
    forgejo_repo: ForgejoRepo,
) -> None:
    """get_urls() yields an aneksajo annex/objects URL (API-detected)."""
    da = DatasetAdapter(str(forgejo_repo.local_path), caching=False)
    urls = list(da.get_urls(forgejo_repo.annex_key))

    annex_urls = [u for u in urls if "/annex/objects/" in u]
    assert annex_urls, f"No annex/objects URL found in {urls}"

    resp = requests.get(annex_urls[0], timeout=10)
    assert resp.status_code == 200
    assert resp.content == forgejo_repo.content


@pytest.mark.ai_generated
def test_open_via_forgejo(
    forgejo_repo: ForgejoRepo,
) -> None:
    """DatasetAdapter.open() reads file content via the Forgejo URL."""
    # Drop local content so open() must fetch remotely
    subprocess.run(
        ["git", "annex", "drop", "--force", forgejo_repo.relpath],
        cwd=forgejo_repo.local_path,
        check=True,
        capture_output=True,
    )

    da = DatasetAdapter(str(forgejo_repo.local_path), caching=False)
    with da.open(forgejo_repo.relpath) as f:
        assert f.read() == forgejo_repo.content

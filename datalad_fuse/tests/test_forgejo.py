"""Integration tests for Forgejo-aneksajo support.

These tests require a container runtime (podman or docker) and will
auto-skip if none is available.  See CONTRIBUTING.md for details on
controlling the test container via environment variables.
"""

from __future__ import annotations

import subprocess

import pytest
import requests

from datalad_fuse.adapter import DatasetAdapter, _is_aneksajo

from .conftest_forgejo import (
    ForgejoInstance,
    ForgejoRepo,
    _find_container_runtime,
    _make_external_instance,
    _map_to_external_address,
    _resolve_user_from_token,
    _skip_or_fail,
)

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


# -- Unit tests for conftest helpers ------------------------------------------
# These exercise pure / near-pure helpers without needing a fresh container,
# providing coverage for the External-URL code path and a few small utility
# functions.


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "url,instance_url,expected",
    [
        # plain clone URL — host and port both swapped
        (
            "http://localhost:3000/admin/foo.git",
            "http://127.0.0.1:54321",
            "http://127.0.0.1:54321/admin/foo.git",
        ),
        # https + trailing slash on instance_url (must be stripped)
        (
            "https://localhost:3000/x/y.git",
            "https://example.com:8080/",
            "https://example.com:8080/x/y.git",
        ),
        # only the first scheme://authority is rewritten — query strings
        # containing other URLs are preserved as-is.
        (
            "http://localhost:3000/a/b/c?u=http://elsewhere/",
            "http://h:1",
            "http://h:1/a/b/c?u=http://elsewhere/",
        ),
        # compound scheme (annex+http) — used for git-annex annexurl.
        # Scheme is preserved; only the authority is swapped.
        (
            "annex+http://localhost:3000/git-annex-p2phttp",
            "http://127.0.0.1:54321",
            "annex+http://127.0.0.1:54321/git-annex-p2phttp",
        ),
        # already-external URL — no-op (relevant for DATALAD_TESTS_FORGEJO_URL).
        (
            "https://hub.datalad.org/test-resources/foo.git",
            "https://hub.datalad.org",
            "https://hub.datalad.org/test-resources/foo.git",
        ),
    ],
)
def test_map_to_external_address(url: str, instance_url: str, expected: str) -> None:
    assert _map_to_external_address(url, instance_url) == expected


@pytest.mark.ai_generated
def test_resolve_user_from_token_valid(forgejo_instance: ForgejoInstance) -> None:
    """A valid token resolves to the admin user."""
    user = _resolve_user_from_token(forgejo_instance.url, forgejo_instance.api_token)
    assert user == forgejo_instance.admin_user


@pytest.mark.ai_generated
def test_resolve_user_from_token_invalid(
    forgejo_instance: ForgejoInstance,
) -> None:
    """An invalid token raises HTTPError."""
    with pytest.raises(requests.HTTPError):
        _resolve_user_from_token(forgejo_instance.url, "definitely-not-a-real-token")


@pytest.mark.ai_generated
def test_make_external_instance_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No env var → returns None (fixture falls through to container)."""
    monkeypatch.delenv("DATALAD_TESTS_FORGEJO_URL", raising=False)
    monkeypatch.delenv("DATALAD_TESTS_FORGEJO_TOKEN", raising=False)
    assert _make_external_instance() is None


@pytest.mark.ai_generated
def test_make_external_instance_set(
    forgejo_instance: ForgejoInstance,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """URL+token env vars produce a matching ForgejoInstance."""
    monkeypatch.setenv("DATALAD_TESTS_FORGEJO_URL", forgejo_instance.url)
    monkeypatch.setenv("DATALAD_TESTS_FORGEJO_TOKEN", forgejo_instance.api_token)
    instance = _make_external_instance()
    assert instance is not None
    assert instance.url == forgejo_instance.url.rstrip("/")
    assert instance.admin_user == forgejo_instance.admin_user
    assert instance.api_token == forgejo_instance.api_token
    # External instances do not own a container.
    assert instance.runtime is None
    assert instance.container_id is None


@pytest.mark.ai_generated
def test_make_external_instance_url_without_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """URL without token must fail loudly (operator error, not skip)."""
    monkeypatch.setenv("DATALAD_TESTS_FORGEJO_URL", "http://example.invalid")
    monkeypatch.delenv("DATALAD_TESTS_FORGEJO_TOKEN", raising=False)
    with pytest.raises(pytest.fail.Exception, match="DATALAD_TESTS_FORGEJO_TOKEN"):
        _make_external_instance()


@pytest.mark.ai_generated
def test_make_external_instance_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unreachable URL must fail with a useful message."""
    monkeypatch.setenv("DATALAD_TESTS_FORGEJO_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATALAD_TESTS_FORGEJO_TOKEN", "irrelevant")
    with pytest.raises(pytest.fail.Exception, match="Could not reach"):
        _make_external_instance()


@pytest.mark.ai_generated
def test_skip_or_fail_strict() -> None:
    """strict=True raises Failed."""
    with pytest.raises(pytest.fail.Exception, match="boom"):
        _skip_or_fail("boom", strict=True)


@pytest.mark.ai_generated
def test_skip_or_fail_lenient() -> None:
    """strict=False raises Skipped (caught here, not propagated)."""
    with pytest.raises(pytest.skip.Exception, match="boom"):
        _skip_or_fail("boom", strict=False)


@pytest.mark.ai_generated
def test_find_container_runtime_explicit_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DATALAD_TESTS_CONTAINER_RUNTIME=foo with foo on PATH returns 'foo'."""
    monkeypatch.setenv("DATALAD_TESTS_CONTAINER_RUNTIME", "echo")
    # 'echo' is universally on PATH; using it avoids monkeypatching shutil.which
    assert _find_container_runtime(strict=True) == "echo"


@pytest.mark.ai_generated
def test_find_container_runtime_explicit_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit runtime not on PATH → fail (strict) / skip (lenient)."""
    monkeypatch.setenv("DATALAD_TESTS_CONTAINER_RUNTIME", "no-such-runtime-xyz")
    with pytest.raises(pytest.fail.Exception, match="no-such-runtime-xyz"):
        _find_container_runtime(strict=True)
    with pytest.raises(pytest.skip.Exception, match="no-such-runtime-xyz"):
        _find_container_runtime(strict=False)

"""Tests for S3 exporttree URL fallback in DatasetAdapter.

Tests cover:
- remote.log parsing (_get_exporttree_remotes)
- S3 version listing via boto3 (_list_s3_versions)
- Version matching by size (_match_s3_version)
- URL construction (get_exporttree_urls)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datalad_fuse.fsspec import DatasetAdapter
from datalad_fuse.utils import AnnexKey


# --- remote.log parsing ---


SAMPLE_REMOTE_LOG = (  # noqa: B950
    "66a7004d-a15e-4764-90cd-54bbd179f74a"
    " autoenable=true bucket=openneuro-private datacenter=US"
    " encryption=none exporttree=yes fileprefix=ds001473/"
    " host=s3.amazonaws.com name=s3-PRIVATE partsize=1GiB"
    " port=80 public=no publicurl=no storageclass=STANDARD"
    " type=S3 versioning=yes timestamp=1541104695.225259256s\n"
    "e28d70a7-9314-4542-a4ce-7d95b862070f"
    " autoenable=true bucket=openneuro.org datacenter=US"
    " encryption=none exporttree=yes fileprefix=ds000113/"
    " host=s3.amazonaws.com name=s3-PUBLIC partsize=1GiB"
    " port=80 public=yes"
    " publicurl=https://s3.amazonaws.com/openneuro.org"
    " storageclass=STANDARD type=S3 versioning=yes"
    " timestamp=1597694369.712881988s\n"
    "b8b60a40-f339-4ddc-b08a-2a6f645bd3ef"
    " timestamp=1541104695.225259256s\n"
    "00000000-0000-0000-0000-000000000001"
    " type=web timestamp=1234567890s\n"
)


@pytest.fixture
def adapter():
    """Create a DatasetAdapter with mocked internals for unit testing."""
    da = object.__new__(DatasetAdapter)
    da.path = "/fake/dataset"
    da.annex = None
    da.caching = False
    return da


@pytest.mark.ai_generated
def test_get_exporttree_remotes_parsing(adapter):
    """Parse remote.log with mixed remote types."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(stdout=SAMPLE_REMOTE_LOG, returncode=0)
        remotes = adapter._get_exporttree_remotes()

    # Only s3-PUBLIC should match (has publicurl starting with http)
    assert len(remotes) == 1
    assert remotes[0]["uuid"] == "e28d70a7-9314-4542-a4ce-7d95b862070f"
    assert remotes[0]["publicurl"] == "https://s3.amazonaws.com/openneuro.org"
    assert remotes[0]["fileprefix"] == "ds000113/"
    assert remotes[0]["bucket"] == "openneuro.org"
    assert remotes[0]["host"] == "s3.amazonaws.com"


@pytest.mark.ai_generated
def test_get_exporttree_remotes_no_remotes(adapter):
    """Empty list when no exporttree remotes exist."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            stdout="b8b60a40 timestamp=1234s\n", returncode=0
        )
        remotes = adapter._get_exporttree_remotes()

    assert remotes == []


@pytest.mark.ai_generated
def test_get_exporttree_remotes_git_failure(adapter):
    """Graceful handling when git-annex branch is not available."""
    import subprocess

    with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "git")):
        remotes = adapter._get_exporttree_remotes()

    assert remotes == []


# --- S3 version listing (boto3) ---


SAMPLE_BOTO3_VERSIONS_RESPONSE = {
    "Versions": [
        {
            "Key": "ds000113/sub-01/anat/sub-01_T1w.nii.gz",
            "VersionId": "abc123",
            "IsLatest": True,
            "Size": 12345678,
            "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
        },
        {
            "Key": "ds000113/sub-01/anat/sub-01_T1w.nii.gz",
            "VersionId": "def456",
            "IsLatest": False,
            "Size": 9999999,
            "ETag": '"aabbccdd00112233"',
        },
        {
            # Different key — should be filtered out
            "Key": "ds000113/sub-01/anat/sub-01_T1w.nii.gz.bak",
            "VersionId": "ignored",
            "IsLatest": True,
            "Size": 12345678,
            "ETag": '"ffffffff"',
        },
    ]
}


@pytest.mark.ai_generated
def test_list_s3_versions_parsing():
    """Parse boto3 list_object_versions response correctly."""
    mock_client = MagicMock()
    mock_client.list_object_versions.return_value = SAMPLE_BOTO3_VERSIONS_RESPONSE

    with patch("boto3.client", return_value=mock_client):
        versions = DatasetAdapter._list_s3_versions(
            "openneuro.org",
            "ds000113/sub-01/anat/sub-01_T1w.nii.gz",
        )

    # Should have 2 versions (the .bak key is filtered out)
    assert len(versions) == 2
    assert versions[0]["VersionId"] == "abc123"
    assert versions[0]["Size"] == 12345678
    assert versions[0]["IsLatest"] is True
    assert versions[1]["VersionId"] == "def456"
    assert versions[1]["Size"] == 9999999


@pytest.mark.ai_generated
def test_list_s3_versions_network_error():
    """Graceful handling of network errors."""
    mock_client = MagicMock()
    mock_client.list_object_versions.side_effect = Exception("timeout")

    with patch("boto3.client", return_value=mock_client):
        versions = DatasetAdapter._list_s3_versions(
            "openneuro.org",
            "ds000113/sub-01/anat/sub-01_T1w.nii.gz",
        )

    assert versions == []


@pytest.mark.ai_generated
def test_list_s3_versions_custom_host():
    """Verify custom S3 endpoint host is used."""
    mock_client = MagicMock()
    mock_client.list_object_versions.return_value = {"Versions": []}

    with patch("boto3.client", return_value=mock_client) as mock_factory:
        DatasetAdapter._list_s3_versions(
            "my-bucket",
            "some/key.txt",
            host="storage.googleapis.com",
        )

    call_kwargs = mock_factory.call_args
    assert call_kwargs[1]["endpoint_url"] == "https://storage.googleapis.com"


# --- Version matching ---


@pytest.mark.ai_generated
def test_match_s3_version_single():
    """One version matches size — return it."""
    versions = [
        {"VersionId": "abc", "Size": 100, "ETag": '"aaa"', "IsLatest": True},
        {"VersionId": "def", "Size": 200, "ETag": '"bbb"', "IsLatest": False},
    ]
    assert DatasetAdapter._match_s3_version(versions, 100) == "abc"


@pytest.mark.ai_generated
def test_match_s3_version_no_match():
    """No version matches size — return None."""
    versions = [
        {"VersionId": "abc", "Size": 100, "ETag": '"aaa"', "IsLatest": True},
    ]
    assert DatasetAdapter._match_s3_version(versions, 999) is None


@pytest.mark.ai_generated
def test_match_s3_version_same_etag():
    """Multiple versions match size, same ETag — prefer latest."""
    versions = [
        {"VersionId": "old", "Size": 100, "ETag": '"aaa"', "IsLatest": False},
        {"VersionId": "new", "Size": 100, "ETag": '"aaa"', "IsLatest": True},
    ]
    assert DatasetAdapter._match_s3_version(versions, 100) == "new"


@pytest.mark.ai_generated
def test_match_s3_version_same_etag_no_latest():
    """Multiple versions match size, same ETag, none marked latest."""
    versions = [
        {"VersionId": "v1", "Size": 100, "ETag": '"aaa"', "IsLatest": False},
        {"VersionId": "v2", "Size": 100, "ETag": '"aaa"', "IsLatest": False},
    ]
    # Should return first one when no latest
    assert DatasetAdapter._match_s3_version(versions, 100) == "v1"


@pytest.mark.ai_generated
def test_match_s3_version_different_etags():
    """Multiple versions match size, different ETags — raise ValueError."""
    versions = [
        {"VersionId": "v1", "Size": 100, "ETag": '"aaa"', "IsLatest": False},
        {"VersionId": "v2", "Size": 100, "ETag": '"bbb"', "IsLatest": True},
    ]
    with pytest.raises(ValueError, match="Ambiguous S3 versions"):
        DatasetAdapter._match_s3_version(versions, 100)


@pytest.mark.ai_generated
def test_match_s3_version_empty():
    """Empty version list — return None."""
    assert DatasetAdapter._match_s3_version([], 100) is None


# --- URL construction ---


@pytest.mark.ai_generated
def test_get_exporttree_urls_construction(adapter):
    """Verify URL construction with version matching."""
    key = AnnexKey(backend="SHA256E", name="abc123", size=12345678, suffix=".nii.gz")

    versions = [
        {"VersionId": "ver1", "Size": 12345678, "ETag": '"aaa"', "IsLatest": True},
    ]

    with (
        patch.object(
            adapter,
            "_get_exporttree_remotes",
            return_value=[
                {
                    "uuid": "test-uuid",
                    "publicurl": "https://s3.amazonaws.com/openneuro.org",
                    "fileprefix": "ds000113/",
                    "bucket": "openneuro.org",
                    "host": "s3.amazonaws.com",
                }
            ],
        ),
        patch.object(
            DatasetAdapter, "_list_s3_versions", return_value=versions
        ),
    ):
        urls = list(adapter.get_exporttree_urls("sub-01/anat/sub-01_T1w.nii.gz", key))

    assert len(urls) == 1
    assert urls[0] == (
        "https://s3.amazonaws.com/openneuro.org/"
        "ds000113/sub-01/anat/sub-01_T1w.nii.gz"
        "?versionId=ver1"
    )


@pytest.mark.ai_generated
def test_get_exporttree_urls_no_remotes(adapter):
    """Empty when no exporttree remotes configured."""
    key = AnnexKey(backend="SHA256E", name="abc123", size=100, suffix=".nii.gz")

    with patch.object(adapter, "_get_exporttree_remotes", return_value=[]):
        urls = list(adapter.get_exporttree_urls("sub-01/anat/sub-01_T1w.nii.gz", key))

    assert urls == []


@pytest.mark.ai_generated
def test_get_exporttree_urls_no_size_fallback(adapter):
    """Falls back to unversioned URL when key has no size."""
    key = AnnexKey(backend="SHA256", name="abc123", size=None)

    with patch.object(
        adapter,
        "_get_exporttree_remotes",
        return_value=[
            {
                "uuid": "test-uuid",
                "publicurl": "https://s3.amazonaws.com/openneuro.org",
                "fileprefix": "ds000113/",
                "bucket": "openneuro.org",
                "host": "s3.amazonaws.com",
            }
        ],
    ):
        urls = list(adapter.get_exporttree_urls("sub-01/anat/sub-01_T1w.nii.gz", key))

    assert len(urls) == 1
    assert "?versionId=" not in urls[0]
    assert urls[0] == (
        "https://s3.amazonaws.com/openneuro.org/"
        "ds000113/sub-01/anat/sub-01_T1w.nii.gz"
    )


@pytest.mark.ai_generated
def test_get_exporttree_urls_ambiguous_skips(adapter):
    """Ambiguous versions (different ETags) skips that remote."""
    key = AnnexKey(backend="SHA256E", name="abc123", size=100, suffix=".nii.gz")

    versions = [
        {"VersionId": "v1", "Size": 100, "ETag": '"aaa"', "IsLatest": False},
        {"VersionId": "v2", "Size": 100, "ETag": '"bbb"', "IsLatest": True},
    ]

    with (
        patch.object(
            adapter,
            "_get_exporttree_remotes",
            return_value=[
                {
                    "uuid": "test-uuid",
                    "publicurl": "https://s3.amazonaws.com/openneuro.org",
                    "fileprefix": "ds000113/",
                    "bucket": "openneuro.org",
                    "host": "s3.amazonaws.com",
                }
            ],
        ),
        patch.object(
            DatasetAdapter, "_list_s3_versions", return_value=versions
        ),
    ):
        urls = list(adapter.get_exporttree_urls("sub-01/anat/sub-01_T1w.nii.gz", key))

    # Ambiguous → skipped, no URLs yielded
    assert urls == []

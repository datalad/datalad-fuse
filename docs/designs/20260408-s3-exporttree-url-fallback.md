# S3 Exporttree URL Fallback

**Date**: 2026-04-08
**Status**: Planning
**Authors**: Yaroslav Halchenko, Claude

## Summary

Some legacy OpenNeuro datasets lack proper versioned S3 URLs in their git-annex
metadata ([openneuro#3875]). While the underlying issue will be fixed directly in
the datasets, `datalad-fuse` needs a workaround so `DatasetAdapter.open()` can
access files on S3 exporttree remotes when `get_urls()` yields no working URLs.

The workaround constructs candidate URLs from the remote's `publicurl` +
`fileprefix` config, then resolves the correct S3 object version by matching
file size (from the annex key) against available versionIds.

## Context / Problem Statement

### Current `get_urls()` flow (fsspec.py:102-153)

1. **whereis URLs**: Queries `annex.whereis(key)` and yields HTTP URLs from
   `v["urls"]`. Normally S3 remotes DO provide versioned URLs here. However, a
   limited set of legacy datasets have incorrect/missing `git annex export`
   state, so `urls` is empty for those.

2. **Remote URL construction**: Iterates `annex.get_remotes()`, reads
   `remote.{r}.url` from git config, and constructs annex object paths
   (`annex/objects/{hash}/{key}`). S3 special remotes have **no `.url` entry** in
   `.git/config` — they have `annex-s3 = true` and `annex-uuid` only. So they're
   silently skipped.

For the affected datasets, both paths fail, and `open()` raises `IOError`.

### S3 exporttree remotes

With `exporttree=yes`, git-annex exports files to S3 at their **tree path**
(not their annex key hash path). Given remote config:

```
publicurl = https://s3.amazonaws.com/openneuro.org
fileprefix = ds000113/
exporttree = yes
versioning = yes
```

A file at tree path `sub-01/anat/sub-01_T1w.nii.gz` is accessible at:
```
https://s3.amazonaws.com/openneuro.org/ds000113/sub-01/anat/sub-01_T1w.nii.gz
```

This config is stored in the `git-annex` branch's `remote.log`, not in
`.git/config`.

### S3 versioning complication

The unversioned URL may have **multiple object versions** with different content.
The latest version is typically correct for HEAD checkouts, but not necessarily
for older checkout trees. We must:

1. List all S3 versionIds for the object
2. Match by file size (from the annex key, via `AnnexKey.size`)
3. If exactly one size match → use that versionId
4. If multiple matches with the **same ETag** → any is fine (identical content
   uploaded multiple times, thus multiple versionIds)
5. If multiple matches with **different ETags** → error (ambiguous, don't guess)
6. If zero matches → no usable version

### Affected datasets

A limited set of legacy OpenNeuro datasets where `s3-PUBLIC` has
`exporttree=yes` but per-file versioned URLs were not properly registered.
Confirmed: ds000113, ds001499, ds001506, ds006623. See [openneuro#3875] for
upstream fix.

[openneuro#3875]: https://github.com/OpenNeuroOrg/openneuro/issues/3875

### Key design constraint

`get_urls(key)` receives an **annex key**, but exporttree URLs require the
**tree path**. The tree path is available in `open(relpath)` which calls
`get_urls(str(key))`. So the fix must either:
- Pass `relpath` through to URL construction, or
- Add a separate exporttree URL method called from `open()`

## Proposed Solution

### 1. Add `_get_exporttree_remotes()` to `DatasetAdapter`

Lazily parse `git-annex:remote.log` once per `DatasetAdapter` instance (cached).
Extract S3 remotes with `publicurl` (not `"no"`) and `exporttree=yes`.

```python
@methodtools.lru_cache(maxsize=1)
def _get_exporttree_remotes(self) -> list[dict[str, str]]:
    """Get S3 exporttree remotes with public URLs.

    Parses git-annex branch remote.log once (cached).

    Returns:
        List of dicts with keys: uuid, publicurl, fileprefix, bucket
    """
```

`remote.log` format (one remote per line):
```
UUID key1=value1 key2=value2 ... timestamp=Ns
```

Filter criteria: `type=S3` AND `publicurl` starts with `http` AND `exporttree=yes`.

### 2. Add `_list_s3_versions()` to `DatasetAdapter`

Query S3 for all versions of an object at a given key prefix.

```python
def _list_s3_versions(
    self, bucket_url: str, object_key: str
) -> list[dict[str, str]]:
    """List all S3 object versions for a key.

    Uses S3 REST API: GET /{bucket}?versions&prefix={key}

    Args:
        bucket_url: S3 bucket URL (e.g., https://s3.amazonaws.com/openneuro.org)
        object_key: Full object key (e.g., ds000113/sub-01/.../bold.nii.gz)

    Returns:
        List of dicts with keys: VersionId, Size, ETag, IsLatest
    """
```

### 3. Add `_match_s3_version()` to `DatasetAdapter`

Match the correct version by file size from the annex key.

```python
def _match_s3_version(
    self, versions: list[dict[str, str]], expected_size: int
) -> str | None:
    """Match S3 object version by file size.

    Args:
        versions: S3 version list from _list_s3_versions()
        expected_size: Expected file size from AnnexKey.size

    Returns:
        versionId string, or None if no match

    Raises:
        ValueError: If multiple versions match size with different ETags
            (ambiguous — refuse to guess)
    """
```

Algorithm:
1. Filter versions where `int(Size) == expected_size`
2. If 0 → return None
3. If 1 → return its VersionId
4. If N, collect unique ETags among matches
5. If all same ETag → return any VersionId (prefer IsLatest=true)
6. If different ETags → raise ValueError (ambiguous)

### 4. Add `get_exporttree_urls(relpath, key)` to `DatasetAdapter`

```python
def get_exporttree_urls(
    self, relpath: str, key: AnnexKey
) -> Iterator[str]:
    """Yield versioned URLs for file on S3 exporttree remotes.

    Args:
        relpath: File path relative to dataset root (tree path)
        key: AnnexKey with expected size for version matching

    Yields:
        Versioned HTTP URLs: {publicurl}/{fileprefix}{relpath}?versionId={id}
    """
```

Logic:
1. Get cached exporttree remote configs
2. For each remote:
   a. Construct object key: `{fileprefix}{relpath}`
   b. List S3 versions for that key
   c. Match version by `key.size`
   d. Yield `{publicurl}/{fileprefix}{relpath}?versionId={matched_id}`
3. If version listing fails or no match, fall back to unversioned URL
   (latest version) with a warning

### 5. Modify `open()` to try exporttree URLs as fallback

In `open()` (line 177-198), after `get_urls(key)` exhausts all candidates
without success, try `get_exporttree_urls(relpath, key)` before raising `IOError`:

```python
if fstate is FileState.NO_CONTENT:
    lgr.debug("%s: opening via fsspec", relpath)
    for url in self.get_urls(str(key)):
        try:
            ...
            return self.fs.open(url, mode, **kwargs)
        except FileNotFoundError as e:
            ...

    # Fallback: try S3 exporttree URLs (workaround for datasets
    # lacking proper versioned URLs — see openneuro#3875)
    if key is not None:
        for url in self.get_exporttree_urls(relpath, key):
            try:
                lgr.debug("%s: Attempting exporttree URL %s", relpath, url)
                return self.fs.open(url, mode, **kwargs)
            except FileNotFoundError as e:
                lgr.debug(
                    "Failed to open file %s at exporttree URL %s: %s",
                    relpath, url, str(e)
                )

    raise IOError(
        f"Could not find a usable URL for {relpath} within {self.path}"
    )
```

## Implementation Details

### Parsing `remote.log`

Use datalad's existing git infrastructure rather than raw subprocess where
possible. Fallback:

```python
import subprocess

result = subprocess.run(
    ["git", "-C", str(self.path), "show", "git-annex:remote.log"],
    capture_output=True, text=True, check=True,
)
```

Each line: `UUID key=value key=value ... timestamp=...s`

Parse by splitting on spaces, then splitting each token on first `=`.
Handle edge cases:
- Multiple `timestamp=` entries (take last)
- Values with `=` in them (split on first `=` only)
- Empty/comment lines

### S3 version listing

Use the S3 REST API (works for public buckets without auth):

```
GET https://s3.amazonaws.com/{bucket}?versions&prefix={object_key}
```

Returns XML with `<Version>` elements containing `Key`, `VersionId`, `Size`,
`ETag`, `IsLatest`, `LastModified`.

Use the existing `aiohttp`/`fsspec` HTTP infrastructure — or a simple
synchronous `urllib.request` since this is a metadata query, not a data transfer.

Parse with `xml.etree.ElementTree` (stdlib, no new dependencies).

### URL construction

```python
publicurl = config["publicurl"].rstrip("/")
fileprefix = config.get("fileprefix", "")
object_key = f"{fileprefix}{relpath}"
# Unversioned
url = f"{publicurl}/{object_key}"
# Versioned
url = f"{publicurl}/{object_key}?versionId={version_id}"
```

Note: `fileprefix` in OpenNeuro already includes trailing slash (e.g.,
`ds000113/`). Be defensive — handle both with and without.

## Test Plan

### Unit tests

1. **`test_get_exporttree_remotes_parsing`**: Mock `remote.log` content with
   mixed remote types (S3 with publicurl, S3 without, directory, web). Verify
   only the correct remotes are returned.

2. **`test_get_exporttree_urls_construction`**: Given known remote config, verify
   URL construction for various relpaths.

3. **`test_get_exporttree_urls_no_remotes`**: Verify empty iterator when no
   exporttree remotes exist (no-op for non-S3 datasets).

4. **`test_match_s3_version_single`**: One version matches size → return it.

5. **`test_match_s3_version_same_etag`**: Multiple versions match size, same
   ETag → return any (prefer latest).

6. **`test_match_s3_version_different_etags`**: Multiple versions match size,
   different ETags → raise ValueError.

7. **`test_match_s3_version_no_match`**: No version matches size → return None.

8. **`test_open_falls_back_to_exporttree`**: Mock `get_urls()` to yield no
   working URLs, mock `get_exporttree_urls()` to yield a working URL. Verify
   `open()` succeeds via the fallback.

9. **`test_list_s3_versions_parsing`**: Mock S3 XML response, verify parsing of
   VersionId, Size, ETag fields.

### Integration tests (if feasible)

Test against an actual OpenNeuro dataset clone (e.g., ds000113) — requires
network access and a populated `.git/annex` branch.

## Alternatives Considered

### A. Extend `get_urls(key, relpath=None)`

Add optional `relpath` parameter to `get_urls()`. Pro: single method. Con:
changes the API signature, mixes two different URL resolution strategies (key-based
vs path-based) in one method.

### B. Use `git annex info REMOTE --json` per remote

Pro: uses official git-annex interface. Con: one subprocess call per remote (3-5
calls per dataset), slower than parsing `remote.log` once.

### C. Use DataLad's `AnnexRepo` API for special remote config

Pro: no raw subprocess calls. Con: unclear if the API exposes `publicurl` /
`exporttree` from the git-annex branch — these aren't standard git config values.
Would need investigation into DataLad internals.

### D. Skip version matching, just use unversioned URL

Pro: much simpler. Con: wrong content for older checkout trees. The user may be
working with a historical commit where the latest S3 version doesn't match.

### E. Fix upstream in OpenNeuro only (no datalad-fuse change)

Being pursued ([openneuro#3875]) but will take time to fix all legacy datasets.
The datalad-fuse workaround enables immediate access.

## Success Criteria

1. `DatasetAdapter.open("sub-01/anat/sub-01_T1w.nii.gz")` succeeds for
   datasets with S3 exporttree remotes (e.g., OpenNeuro ds000113)
2. Correct version selected: file size matches `AnnexKey.size`
3. Ambiguous versions (different ETags, same size) produce a clear error
4. No performance regression for datasets that already have working URLs
   (exporttree lookup is lazy/cached and only triggered as fallback)
5. All existing tests continue to pass
6. New unit tests cover parsing, version matching, URL construction, and
   fallback behavior

## References

- [openneuro#3875]: S3 VersionId'ed URLs missing from git-annex
- OpenNeuro S3 remote config: `bucket=openneuro.org`, `publicurl=https://s3.amazonaws.com/openneuro.org`, `exporttree=yes`
- git-annex special remote protocol: https://git-annex.branchable.com/special_remotes/
- git-annex exporttree: https://git-annex.branchable.com/git-annex-export/
- S3 ListObjectVersions API: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html

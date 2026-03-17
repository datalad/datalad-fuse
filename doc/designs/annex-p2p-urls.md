# git-annex p2p HTTP proxy URL support for datalad-fuse

## Background

git-annex remotes can expose file content via p2p HTTP proxy endpoints.
When a remote has `annexurl` and `annex-uuid` configured in `.git/config`:

```ini
[remote "origin"]
    url = https://hub.psychoinformatics.de/orinoco/psyinf-pool-files-public
    annexurl = annex+https://hub.psychoinformatics.de:443/git-annex-p2phttp
    annex-uuid = cedde37c-416a-41fd-87e0-4ac100df4bf7
```

a p2p proxy URL for any key can be constructed as:

```
{annexurl_without_annex+_prefix}/git-annex/{annex-uuid}/key/{KEY}
```

Example:
```
https://hub.psychoinformatics.de/git-annex-p2phttp/git-annex/cedde37c-416a-41fd-87e0-4ac100df4bf7/key/SHA256E-s84738--3f1e6bab9005d4950a2080e2692ab29257b6bd7b4e78787a4720d9e419fe306f.jpg
```

This enables transparent file access from remotes that don't have direct
HTTP URLs for individual files — the p2p proxy API provides content by key.

## Investigation results (2026-03-17)

### HTTP behavior of p2p endpoints

Tested against `hub.psychoinformatics.de` (Forgejo + Caddy):

| Feature | Behavior |
|---------|----------|
| `GET` | Works, returns full file content with `Transfer-Encoding: chunked` |
| `HEAD` | Returns **404** (not supported by current Forgejo deployment) |
| `Content-Length` | **Not sent**; size in custom `X-Git-Annex-Data-Length` header |
| `Range` requests | **Not supported**; server ignores Range header, returns full content with 200 |
| Content type | `application/octet-stream` |

Data integrity was verified — the SHA256 hash of the downloaded content
matches the key, and the file size matches the `s` field in the key name.

### fsspec behavior

- Without explicit `size=`: fsspec creates `HTTPStreamFile` (non-seekable),
  because it can't determine file size from HEAD (404) or `Content-Length`
  (absent). This breaks FUSE reads which require `seek()`.

- With `size=N` passed to `fs.open()`: fsspec creates a seekable `HTTPFile`.
  The size can be extracted from `AnnexKey.size` (already parsed in
  `datalad_fuse/utils.py`).

- Range requests are silently ignored — fsspec's block-based reads at
  offset 0 work (full content is returned), but reads at offset > 0 would
  get the wrong data. Setting `block_size=file_size` ensures a single full
  fetch.

### Related upstream issues

- [git-annex bug: recent p2phttp silently refuses connections](https://git-annex.branchable.com/bugs/recent_annex_p2phttp_silently___40__in_--debug__41___refuses/)
  — Regression in git-annex 10.20260316+ where p2phttp server silently
  drops connections. Works in 10.20251029.

- [git-annex todo: add Range (in) and Content-Length (out) headers](https://git-annex.branchable.com/todo/p2p__58___add_Range___40__in__41___and_Content-Length___40__out__41___hdrs/)
  — Feature request to add `Range` request support and `Content-Length`
  response header to the p2p protocol. Would enable sparse/partial file
  access. Currently acknowledged as possible but unimplemented.

- [forgejo-aneksajo#111: HEAD request support](https://codeberg.org/forgejo-aneksajo/forgejo-aneksajo/issues/111)
  — HEAD requests return 404 on the Forgejo deployment while they work on
  local `git annex p2phttp` servers. May be a Forgejo-specific issue.

Once the upstream issues are resolved (Range + Content-Length + HEAD
support), the p2p endpoints would behave like standard HTTP and require
no special handling in datalad-fuse. The implementation below is designed
to work with the current limitations and adapt gracefully as upstream
improves.

## Implementation plan

### 1. Modify `DatasetAdapter.get_urls()` (`datalad_fuse/fsspec.py:102-153`)

Change return type from `Iterator[str]` to `Iterator[tuple[str, bool]]`
where the bool indicates `is_p2p`.

In the remote iteration loop, also read `remote.{r}.annexurl` via
`self.annex.config.get()`. If it starts with `annex+`, strip the prefix
and store in a `uuid2annexurl` dict. Allow remotes with only `annexurl`
(no `url`) to still be processed.

Yield URLs in priority order:
1. Direct URLs from `whereis` → `(url, False)`
2. **p2p proxy URLs** → `(f"{base}/git-annex/{uuid}/key/{key}", True)`
3. Hash-path constructed URLs → `(url, False)`

### 2. Modify `DatasetAdapter.open()` (`datalad_fuse/fsspec.py:155-201`)

Update URL iteration to unpack tuples. For p2p URLs:

- **Bypass `CachingFileSystem`**: open directly on the underlying
  `HTTPFileSystem` (`self.fs.fs` when caching, `self.fs` otherwise).
  Reason: `CachingFileSystem._open()` passes its own `size=` kwarg to the
  underlying FS; passing ours through `**kwargs` causes `TypeError:
  multiple values for argument 'size'`.

- **Pass `size=key.size`**: so fsspec creates seekable `HTTPFile` instead
  of `HTTPStreamFile`.

- **Skip if `key.size` is None**: without known size, p2p can't work
  (HEAD returns 404, so size can't be discovered).

For regular URLs, keep current behavior unchanged.

### 3. Add tests (`datalad_fuse/tests/test_p2p_urls.py`)

Unit tests using mocks:
- p2p URL construction format and ordering
- `annexurl` without `annex+` prefix is ignored
- Remote with only `annexurl` (no `url`) still yields p2p URLs
- `size=key.size` is passed for p2p opens
- p2p opens bypass CachingFileSystem

### Key observations

- **`AnnexKey.size`** (`utils.py`) already parses size from key names
  (`SHA256E-s84738--...` → 84738), no changes needed.

- **`self.annex.config.get()`** reads standard git config, so `annexurl`
  is accessible.

- **No changes needed** to `utils.py`, `fuse_.py`, or `consts.py`.

- Since p2p endpoints don't support Range requests, the "sparse download"
  benefit of datalad-fuse is lost for these URLs — full files are always
  fetched. But it still enables transparent access without `datalad get`.

- Once upstream adds Range + Content-Length support, the `is_p2p=True`
  path could be simplified or removed entirely — the standard fsspec HTTP
  handling would just work.

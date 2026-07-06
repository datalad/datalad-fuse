"""Backend-agnostic adapter layer for remote file access."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from enum import Enum
from itertools import chain
import json
import logging
import os
import os.path
from pathlib import Path
import subprocess
from types import TracebackType
from typing import IO, Any, Optional, Tuple, cast
from urllib.parse import urlparse
import urllib.request

import boto3
from botocore import UNSIGNED
from botocore.config import Config as BotocoreConfig
from datalad import cfg
from datalad.distribution.dataset import Dataset
from datalad.support.annexrepo import AnnexRepo
from datalad.utils import get_dataset_root
import methodtools

from .backends import DEFAULT_BACKENDS, Backend
from .consts import CACHE_SIZE
from .fsspec import FsspecBackend
from .remfile import RemfileBackend
from .utils import AnnexKey, is_annex_dir_or_key

lgr = logging.getLogger("datalad.fuse.adapter")

FileState = Enum("FileState", "NOT_ANNEXED NO_CONTENT HAS_CONTENT")


# ---------------------------------------------------------------------------
# Backend creation helpers
# ---------------------------------------------------------------------------


def resolve_backends(backends: Optional[str] = None) -> tuple[str, bool]:
    """Resolve backends spec from *backends* argument, config, or default.

    Returns ``(spec, explicit)`` where ``explicit`` is True when the user (or
    config) supplied the spec and False when falling back to
    :data:`DEFAULT_BACKENDS`.  Callers use ``explicit`` to decide whether a
    missing backend is a warning (explicit) or a silent skip (default).
    """
    if backends is not None:
        return backends, True
    from_cfg = cfg.get("datalad.fusefs.backends", None)
    if from_cfg is not None:
        return str(from_cfg), True
    return DEFAULT_BACKENDS, False


def create_backends(
    spec: str, path: str | Path, caching: bool, explicit: bool = True
) -> list[Backend]:
    """Instantiate backends from a comma-separated *spec*.

    Backends that cannot be imported are skipped; missing backends requested
    via an *explicit* spec (user argument or config) are logged as warnings,
    while those missing from the default spec are logged at debug level only.
    Raises ``ValueError`` if no usable backend remains.
    """
    backends: list[Backend] = []
    for name in spec.split(","):
        name = name.strip()
        if not name:
            # tolerate stray commas / whitespace, e.g. "remfile,,fsspec"
            continue
        try:
            if name == "fsspec":
                backends.append(FsspecBackend(path, caching))
            elif name == "remfile":
                backends.append(RemfileBackend())
            else:
                raise ValueError(f"Unknown backend: {name!r}")
        except ImportError as e:
            if explicit:
                lgr.warning(
                    "Backend %r requested but not available (%s); skipping. "
                    "Install its package or adjust the --backends spec.",
                    name,
                    e,
                )
            else:
                lgr.debug("Backend %r not available (not installed), skipping", name)
    if not backends:
        raise ValueError(
            f"No usable backends from spec {spec!r}. "
            "Install missing packages or adjust --backends / "
            "datalad.fusefs.backends config."
        )
    return backends


def is_http_url(s: str) -> bool:
    return s.lower().startswith(("http://", "https://"))


_aneksajo_cache: dict[str, bool] = {}


def _is_aneksajo(base_url: str) -> bool:
    """Check if a URL points to a Forgejo-aneksajo instance.

    Probes ``{scheme}://{host}/api/forgejo/v1/version`` and checks whether
    the version string contains ``git-annex``, which indicates the
    forgejo-aneksajo fork.

    Results are cached per ``scheme://host:port`` for the process lifetime.
    """
    parsed = urlparse(base_url)
    # Cache key without userinfo so credentials don't fragment the cache
    host = parsed.hostname or ""
    port_suffix = f":{parsed.port}" if parsed.port else ""  # noqa: E231
    cache_key = f"{parsed.scheme}://{host}{port_suffix}"  # noqa: E231

    if cache_key in _aneksajo_cache:
        return _aneksajo_cache[cache_key]

    try:
        api_url = f"{cache_key}/api/forgejo/v1/version"
        req = urllib.request.Request(api_url, method="GET")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            result = "git-annex" in data.get("version", "")
    except Exception:
        lgr.debug("_is_aneksajo(%s) probe failed", cache_key, exc_info=True)
        result = False

    _aneksajo_cache[cache_key] = result
    lgr.debug("_is_aneksajo(%s) = %s", cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Dataset / Adapter layer
# ---------------------------------------------------------------------------


class DatasetAdapter:
    def __init__(
        self,
        path: str | Path,
        caching: bool,
        mode_transparent: bool = False,
        backends: Optional[str] = None,
    ) -> None:
        self.path = Path(path)
        self.mode_transparent = mode_transparent
        ds = Dataset(path)
        self.annex: Optional[AnnexRepo]
        if isinstance(ds.repo, AnnexRepo):
            self.annex = ds.repo
        else:
            self.annex = None
        self.commit_dt = datetime.fromtimestamp(
            ds.repo.get_commit_date(), tz=timezone.utc
        )
        spec, explicit = resolve_backends(backends)
        self._backends = create_backends(spec, path, caching, explicit=explicit)

    def close(self) -> None:
        if self.annex is not None:
            self.annex._batched.clear()

    @methodtools.lru_cache(maxsize=CACHE_SIZE)
    def get_file_state(self, relpath: str) -> tuple[FileState, Optional[AnnexKey]]:
        p = self.path / relpath
        lgr.debug("get_file_state: %s", relpath)

        def handle_path_under_annex_objects(
            p: Path,
        ) -> tuple[FileState, Optional[AnnexKey]]:
            iadok = is_annex_dir_or_key(p)
            if isinstance(iadok, AnnexKey):
                if p.exists():
                    return (FileState.HAS_CONTENT, iadok)
                else:
                    return (FileState.NO_CONTENT, iadok)
            else:
                return (FileState.NOT_ANNEXED, None)

        # Shortcut handling of content under .git, in particular - annex key paths
        if self.mode_transparent and relpath.startswith(".git/"):
            return handle_path_under_annex_objects(p)

        # A regular file or git link for which we need to explicitly ask annex about
        if not p.is_symlink():
            if p.stat().st_size < 1024 and self.annex is not None:
                if self.annex.is_under_annex(relpath, batch=True):
                    key = AnnexKey.parse(self.annex.get_file_key(relpath, batch=True))
                    if self.annex.file_has_content(relpath, batch=True):
                        return (FileState.HAS_CONTENT, key)
                    else:
                        return (FileState.NO_CONTENT, key)
            return (FileState.NOT_ANNEXED, None)

        return handle_path_under_annex_objects(
            Path(os.path.normpath(p.parent / os.readlink(p)))
        )

    def get_urls(self, key: str) -> Iterator[str]:
        assert self.annex is not None
        # TODO: switch to batch=True whenever
        # https://github.com/datalad/datalad/pull/6379 is merged/released.
        # Will need a recent git-annex to work!
        whereis = self.annex.whereis(key, output="full", batch=False, key=True)
        remote_uuids = []
        for ru, v in whereis.items():
            remote_uuids.append(ru)
            for u in v["urls"]:
                if is_http_url(u):
                    yield u

        path_mixed = self.annex._batched.get(
            "examinekey",
            annex_options=["--format=annex/objects/${hashdirmixed}${key}/${key}\\n"],
            path=self.annex.path,
        )(key)
        path_lower = self.annex._batched.get(
            "examinekey",
            annex_options=["--format=annex/objects/${hashdirlower}${key}/${key}\\n"],
            path=self.annex.path,
        )(key)

        uuid2remote_url = {}
        aneksajo_uuids: set[str] = set()
        for r in self.annex.get_remotes():
            if (ru := self.annex.config.get(f"remote.{r}.annex-uuid")) is None:
                continue
            if (remote_url := self.annex.config.get(f"remote.{r}.url")) is None:
                continue
            remote_url = self.annex.config.rewrite_url(remote_url)
            uuid2remote_url[ru] = remote_url
            # Detect Forgejo-aneksajo instances via API probe (cached).
            # TODO: pushurl could be different from url, should also check
            #   remote.{r}.pushurl config
            # TODO: SSH remote URLs not yet supported -- would need to
            #   derive the HTTP base URL from the SSH URL
            if is_http_url(remote_url) and _is_aneksajo(remote_url):
                aneksajo_uuids.add(ru)

        for ru in remote_uuids:
            try:
                base_url = uuid2remote_url[ru]
            except KeyError:
                continue
            if is_http_url(base_url):
                base_stripped = base_url.rstrip("/")
                # Forgejo/Gitea with aneksajo: use annex/objects endpoint
                # which supports HEAD and Range requests.
                # See https://codeberg.org/forgejo-aneksajo/forgejo-aneksajo/issues/111
                if ru in aneksajo_uuids and base_stripped.endswith(".git"):
                    forge_base = base_stripped[:-4].rstrip("/")
                    yield forge_base + "/" + path_lower
                if base_stripped.lower().endswith("/.git"):
                    paths = [path_mixed, path_lower]
                else:
                    paths = [
                        path_lower,
                        path_mixed,
                        f".git/{path_lower}",
                        f".git/{path_mixed}",
                    ]
                for p in paths:
                    yield base_stripped + "/" + p

    @methodtools.lru_cache(maxsize=1)
    def _get_exporttree_remotes(self) -> list[dict[str, str]]:
        """Get S3 exporttree remotes with public URLs.

        Parses the git-annex branch remote.log once (cached per
        DatasetAdapter instance) to find S3 special remotes configured
        with ``exporttree=yes`` and a usable ``publicurl``.

        Workaround for legacy datasets that lack proper versioned S3 URLs in
        their git-annex metadata.  See
        https://github.com/OpenNeuroOrg/openneuro/issues/3875
        """
        try:
            result = subprocess.run(
                ["git", "-C", str(self.path), "show", "git-annex:remote.log"],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError:
            lgr.debug("Could not read git-annex:remote.log for %s", self.path)
            return []

        remotes: list[dict[str, str]] = []
        for line in result.stdout.strip().splitlines():
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            uuid = parts[0]
            config: dict[str, str] = {}
            for token in parts[1:]:
                if "=" in token:
                    k, v = token.split("=", 1)
                    config[k] = v
            if (
                config.get("type") == "S3"
                and config.get("exporttree") == "yes"
                and config.get("publicurl", "no").startswith("http")
            ):
                remotes.append(
                    {
                        "uuid": uuid,
                        "publicurl": config["publicurl"].rstrip("/"),
                        "fileprefix": config.get("fileprefix", ""),
                        "bucket": config.get("bucket", ""),
                        "host": config.get("host", "s3.amazonaws.com"),
                    }
                )
        return remotes

    @staticmethod
    def _list_s3_versions(
        bucket: str,
        object_key: str,
        host: str = "s3.amazonaws.com",
    ) -> list[dict[str, Any]]:
        """List all S3 object versions for *object_key* in *bucket*.

        Uses anonymous credentials (for public buckets).
        """
        try:
            endpoint_url = f"https://{host}"
            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                config=BotocoreConfig(signature_version=UNSIGNED),
            )
            response = client.list_object_versions(Bucket=bucket, Prefix=object_key)
        except Exception as e:
            lgr.debug("Failed to list S3 versions for %s/%s: %s", bucket, object_key, e)
            return []

        versions: list[dict[str, Any]] = []
        for v in response.get("Versions", []):
            # Only include exact key matches (prefix query may return others)
            if v.get("Key") == object_key:
                versions.append(
                    {
                        "VersionId": v.get("VersionId", ""),
                        "Size": v.get("Size", 0),
                        "ETag": v.get("ETag", ""),
                        "IsLatest": v.get("IsLatest", False),
                    }
                )
        return versions

    @staticmethod
    def _match_s3_version(
        versions: list[dict[str, Any]], expected_size: int
    ) -> Optional[str]:
        """Match the correct S3 object version by file size.

        Returns the matched versionId, or ``None`` if no version matches.
        Raises ``ValueError`` if multiple versions match by size but have
        different ETags (ambiguous content — refuse to guess).
        """
        matches = [v for v in versions if v["Size"] == expected_size]
        if not matches:
            return None
        if len(matches) == 1:
            return str(matches[0]["VersionId"])
        # Multiple matches — check ETags
        etags = {v["ETag"] for v in matches}
        if len(etags) == 1:
            # Same content uploaded multiple times; prefer the latest
            for v in matches:
                if v["IsLatest"]:
                    return str(v["VersionId"])
            return str(matches[0]["VersionId"])
        raise ValueError(
            f"Ambiguous S3 versions: {len(matches)} versions match size "
            f"{expected_size} but have {len(etags)} distinct ETags. "
            f"Cannot determine correct version."
        )

    def get_exporttree_urls(self, relpath: str, key: AnnexKey) -> Iterator[str]:
        """Yield versioned URLs for *relpath* on S3 exporttree remotes.

        Workaround for datasets lacking proper versioned URLs in git-annex
        metadata.  Constructs URLs from the remote's ``publicurl`` +
        ``fileprefix`` and resolves the correct S3 object version by matching
        ``key.size``.
        """
        remotes = self._get_exporttree_remotes()
        if not remotes:
            return

        for remote in remotes:
            publicurl = remote["publicurl"]
            fileprefix = remote["fileprefix"]
            bucket = remote["bucket"]
            host = remote["host"]
            object_key = f"{fileprefix}{relpath}"
            base_url = f"{publicurl}/{object_key}"

            if key.size is not None:
                versions = self._list_s3_versions(bucket, object_key, host)
                if versions:
                    try:
                        version_id = self._match_s3_version(versions, key.size)
                    except ValueError as e:
                        lgr.warning("%s: %s", relpath, e)
                        continue
                    if version_id:
                        yield f"{base_url}?versionId={version_id}"
                        continue
                    else:
                        lgr.debug(
                            "%s: no S3 version matches size %d at %s",
                            relpath,
                            key.size,
                            base_url,
                        )
                        continue

            # Fallback: no size info or version listing failed —
            # try unversioned URL (returns latest version).
            lgr.warning(
                "%s: falling back to unversioned S3 URL %s "
                "(cannot verify correct version)",
                relpath,
                base_url,
            )
            yield base_url

    def open(
        self,
        relpath: str,
        mode: str = "rb",
        encoding: str = "utf-8",
        errors: Optional[str] = None,
    ) -> IO:
        if mode not in ("r", "rb", "rt"):
            raise NotImplementedError("Only modes 'r', 'rb', and 'rt' are supported")
        if mode == "rb":
            kwargs: dict[str, Any] = {}
        else:
            kwargs = {"encoding": encoding, "errors": errors}
        fstate, key = self.get_file_state(relpath)
        if fstate is FileState.NOT_ANNEXED:
            lgr.debug("%s: not under annex", relpath)
        else:
            lgr.debug(
                "%s: under annex, %s content",
                relpath,
                "has" if fstate is FileState.HAS_CONTENT else "does not have",
            )
        if fstate is FileState.NO_CONTENT:
            # Walk the backend chain; fall through to next backend on failure
            last_error: Optional[Exception] = None
            for backend in self._backends:
                if not backend.can_handle(key, mode):
                    lgr.debug(
                        "%s: backend %s cannot handle (suffix=%s, mode=%s)",
                        relpath,
                        backend.name,
                        key.suffix if key else None,
                        mode,
                    )
                    continue
                lgr.debug("%s: opening via backend %s", relpath, backend.name)
                # Primary URLs from git-annex whereis / remote paths, then
                # S3 exporttree fallback URLs (legacy openneuro datasets that
                # lack proper versioned URLs in annex metadata).  Lazy so
                # the boto3 calls only happen when primary URLs fail.
                primary_urls = self.get_urls(str(key))
                fallback_urls: Iterator[str] = (
                    self.get_exporttree_urls(relpath, key)
                    if key is not None
                    else iter([])
                )
                for url in chain(primary_urls, fallback_urls):
                    try:
                        lgr.debug(
                            "%s: trying URL %s (backend=%s)",
                            relpath,
                            url,
                            backend.name,
                        )
                        return backend.open_url(url, mode, **kwargs)
                    except FileNotFoundError as e:
                        lgr.debug(
                            "Failed to open %s at URL %s: %s",
                            relpath,
                            url,
                            str(e),
                        )
                        last_error = e
                    except Exception as e:
                        lgr.debug(
                            "%s: backend %s failed at URL %s: %s",
                            relpath,
                            backend.name,
                            url,
                            e,
                        )
                        last_error = e
                # All URLs failed for this backend — try the next one
                lgr.debug(
                    "%s: backend %s exhausted all URLs, trying next",
                    relpath,
                    backend.name,
                )
            # No backend succeeded
            raise IOError(
                f"Could not open {relpath} within {self.path}"
                f" (backends={','.join(b.name for b in self._backends)})"
            ) from last_error
        else:
            lgr.debug("%s: opening directly", relpath)
            return open(self.path / relpath, mode, **kwargs)  # type: ignore[return-value]

    def clear(self) -> None:
        for backend in self._backends:
            backend.clear()


class RemoteFilesystemAdapter:
    """Top-level context manager for accessing remote files within a DataLad dataset.

    Resolves paths to their owning dataset, creates per-dataset
    :class:`DatasetAdapter` instances, and delegates file operations to them.
    """

    def __init__(
        self,
        root: str | Path,
        caching: bool,
        mode_transparent: bool = False,
        backends: Optional[str] = None,
    ) -> None:
        self.root = Path(root)
        self.mode_transparent = mode_transparent
        self.caching = caching
        self.backends = backends
        self.datasets: dict[Path, DatasetAdapter] = {}

    def __enter__(self) -> RemoteFilesystemAdapter:
        return self

    def __exit__(
        self,
        _exc_type: Optional[type[BaseException]],
        _exc_val: Optional[BaseException],
        _exc_tb: Optional[TracebackType],
    ) -> None:
        for ds in self.datasets.values():
            ds.close()
        self.datasets.clear()

    @methodtools.lru_cache(maxsize=CACHE_SIZE)
    # TODO: optimize "caching" more since for all files under the same directory
    # they all would belong to the same dataset
    def get_dataset_path(self, path: str | Path) -> Path:
        path = Path(self.root, path)
        dspath = get_dataset_root(path)
        if dspath is None:
            raise ValueError(f"Path not under DataLad: {path}")
        dspath = Path(dspath)
        assert isinstance(dspath, Path)
        try:
            dspath.relative_to(self.root)
        except ValueError:
            raise ValueError(f"Path not under root dataset: {path}")
        return dspath

    def resolve_dataset(self, filepath: str | Path) -> tuple[DatasetAdapter, str]:
        dspath = self.get_dataset_path(filepath)
        try:
            dsap = self.datasets[dspath]
        except KeyError:
            dsap = self.datasets[dspath] = DatasetAdapter(
                dspath,
                mode_transparent=self.mode_transparent,
                caching=self.caching,
                backends=self.backends,
            )
        relpath = str(Path(filepath).relative_to(dspath))
        return dsap, relpath

    def open(
        self,
        filepath: str | Path,
        mode: str = "rb",
        encoding: str = "utf-8",
        errors: Optional[str] = None,
    ) -> IO:
        dsap, relpath = self.resolve_dataset(filepath)
        lgr.debug(
            "%s: path resolved to %s in dataset at %s", filepath, relpath, dsap.path
        )
        return dsap.open(relpath, mode=mode, encoding=encoding, errors=errors)

    def get_file_state(
        self, filepath: str | Path
    ) -> tuple[FileState, Optional[AnnexKey]]:
        dsap, relpath = self.resolve_dataset(filepath)
        return cast(Tuple[FileState, Optional[AnnexKey]], dsap.get_file_state(relpath))

    def is_under_annex(self, filepath: str | Path) -> bool:
        dsap, relpath = self.resolve_dataset(filepath)
        fstate, _ = dsap.get_file_state(relpath)
        return fstate is not FileState.NOT_ANNEXED

    def get_commit_datetime(self, filepath: str | Path) -> datetime:
        dsap, _ = self.resolve_dataset(filepath)
        return dsap.commit_dt

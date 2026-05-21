from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from enum import Enum
import json
import logging
import os
import os.path
from pathlib import Path
import subprocess
from types import SimpleNamespace, TracebackType
from typing import IO, Any, Optional, Tuple, cast
from urllib.parse import urlparse
import urllib.request

import aiohttp
from aiohttp_retry import ListRetry, RetryClient
import boto3
from botocore import UNSIGNED
from botocore.config import Config as BotocoreConfig
from datalad.distribution.dataset import Dataset
from datalad.support.annexrepo import AnnexRepo
from datalad.utils import get_dataset_root
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.cached import CachingFileSystem
from fsspec.implementations.http import HTTPFileSystem
import methodtools

from .consts import CACHE_SIZE
from .utils import AnnexKey, is_annex_dir_or_key

lgr = logging.getLogger("datalad.fuse.fsspec")

FileState = Enum("FileState", "NOT_ANNEXED NO_CONTENT HAS_CONTENT")


class DatasetAdapter:
    def __init__(
        self, path: str | Path, caching: bool, mode_transparent: bool = False
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
        self.caching = caching
        fs = HTTPFileSystem(get_client=get_client)
        if self.caching:
            self.fs = CachingFileSystem(
                fs=fs,
                # target_protocol='blockcache',
                cache_storage=os.path.join(path, ".git", "datalad", "cache", "fsspec"),
                # cache_check=600,
                # block_size=1024,
                # check_files=True,
                # expiry_times=True,
                # same_names=True
            )
        else:
            self.fs = fs

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

        This is a workaround for legacy datasets that lack proper
        versioned S3 URLs in their git-annex metadata.
        See https://github.com/OpenNeuroOrg/openneuro/issues/3875

        Returns
        -------
        list of dict
            Each dict has keys: ``uuid``, ``publicurl``, ``fileprefix``,
            ``bucket``, ``host``.
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
        """List all S3 object versions for a key.

        Uses ``boto3`` to call ``ListObjectVersions`` with anonymous
        credentials (for public buckets).

        Parameters
        ----------
        bucket : str
            S3 bucket name (e.g., ``openneuro.org``).
        object_key : str
            Full object key including fileprefix (e.g.,
            ``ds000113/sub-01/.../bold.nii.gz``).
        host : str
            S3 endpoint hostname (default: ``s3.amazonaws.com``).

        Returns
        -------
        list of dict
            Each dict has keys: ``VersionId``, ``Size``, ``ETag``,
            ``IsLatest``.
        """
        try:
            endpoint_url = f"https://{host}"
            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                config=BotocoreConfig(signature_version=UNSIGNED),
            )
            response = client.list_object_versions(
                Bucket=bucket, Prefix=object_key
            )
        except Exception as e:
            lgr.debug(
                "Failed to list S3 versions for %s/%s: %s",
                bucket, object_key, e,
            )
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

        Parameters
        ----------
        versions : list of dict
            S3 version list from :meth:`_list_s3_versions`.
        expected_size : int
            Expected file size from ``AnnexKey.size``.

        Returns
        -------
        str or None
            Matched versionId, or ``None`` if no version matches.

        Raises
        ------
        ValueError
            If multiple versions match by size but have different ETags
            (ambiguous content — refuse to guess).
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

    def get_exporttree_urls(
        self, relpath: str, key: AnnexKey
    ) -> Iterator[str]:
        """Yield versioned URLs for file on S3 exporttree remotes.

        Workaround for datasets lacking proper versioned URLs in
        git-annex metadata. Constructs URLs from the remote's
        ``publicurl`` + ``fileprefix`` and resolves the correct S3
        object version by matching ``key.size``.

        Parameters
        ----------
        relpath : str
            File path relative to dataset root (tree path).
        key : AnnexKey
            Annex key with expected file size for version matching.

        Yields
        ------
        str
            Versioned HTTP URLs (``...?versionId=...``) or unversioned
            URLs as fallback.
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
                        version_id = self._match_s3_version(
                            versions, key.size
                        )
                    except ValueError as e:
                        lgr.warning(
                            "%s: %s", relpath, e
                        )
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
            # try unversioned URL (returns latest version)
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
            kwargs = {}
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
            lgr.debug("%s: opening via fsspec", relpath)
            for url in self.get_urls(str(key)):
                try:
                    lgr.debug("%s: Attempting to open via URL %s", relpath, url)
                    return self.fs.open(url, mode, **kwargs)  # type: ignore
                except BlocksizeMismatchError as e:
                    lgr.warning(
                        "%s: Blocksize mismatch: %s; deleting cached file and"
                        " re-opening",
                        relpath,
                        e,
                    )
                    self.fs.pop_from_cache(url)
                    return self.fs.open(url, mode, **kwargs)  # type: ignore
                except FileNotFoundError as e:
                    lgr.debug(
                        "Failed to open file %s at URL %s: %s", relpath, url, str(e)
                    )
            # Fallback: try S3 exporttree URLs (workaround for datasets
            # lacking proper versioned URLs — see openneuro#3875)
            if key is not None:
                for url in self.get_exporttree_urls(relpath, key):
                    try:
                        lgr.debug(
                            "%s: Attempting exporttree URL %s", relpath, url
                        )
                        return self.fs.open(url, mode, **kwargs)  # type: ignore
                    except BlocksizeMismatchError as e:
                        lgr.warning(
                            "%s: Blocksize mismatch: %s; deleting cached file"
                            " and re-opening",
                            relpath,
                            e,
                        )
                        self.fs.pop_from_cache(url)
                        return self.fs.open(url, mode, **kwargs)  # type: ignore
                    except FileNotFoundError as e:
                        lgr.debug(
                            "Failed to open file %s at exporttree URL %s: %s",
                            relpath,
                            url,
                            str(e),
                        )
            raise IOError(
                f"Could not find a usable URL for {relpath} within {self.path}"
            )
        else:
            lgr.debug("%s: opening directly", relpath)
            return open(self.path / relpath, mode, **kwargs)  # type: ignore

    def clear(self) -> None:
        if self.caching:
            self.fs.clear_cache()


class FsspecAdapter:
    def __init__(
        self, root: str | Path, caching: bool, mode_transparent: bool = False
    ) -> None:
        self.root = Path(root)
        self.mode_transparent = mode_transparent
        self.caching = caching
        self.datasets: dict[Path, DatasetAdapter] = {}

    def __enter__(self) -> FsspecAdapter:
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


async def on_request_start(
    _session: aiohttp.ClientSession,
    trace_config_ctx: SimpleNamespace,
    params: aiohttp.TraceRequestStartParams,
) -> None:
    if trace_config_ctx.trace_request_ctx["current_attempt"] > 1:
        lgr.warning("Retrying request to %s", params.url)


async def get_client(**kwargs: Any) -> RetryClient:
    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    return RetryClient(
        client_session=aiohttp.ClientSession(
            trace_configs=[trace_config],
            **kwargs,
        ),
        retry_options=ListRetry(timeouts=[1, 2, 6, 15, 36]),
    )

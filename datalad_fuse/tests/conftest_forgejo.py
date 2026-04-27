"""Pytest fixtures for ephemeral Forgejo-aneksajo container testing.

Starts a disposable Forgejo-aneksajo instance, creates repos with
annexed content, and tears everything down afterwards.

Controlled via environment variables — see CONTRIBUTING.md.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from time import sleep
from typing import Iterator
import uuid as uuid_mod

from datalad.api import Dataset
import pytest
import requests

lgr = logging.getLogger("datalad.fuse.tests.forgejo")

# NB: the bare "forgejo-rootless" tag is the *upstream* Forgejo image,
# not the aneksajo build — pin explicitly to a git-annex tag.
FORGEJO_IMAGE = (
    "codeberg.org/forgejo-aneksajo/forgejo-aneksajo:v14.0.3-git-annex2-rootless"
)
FORGEJO_CONTAINER_NAME = "datalad-fuse-test-forgejo"
FORGEJO_ADMIN_USER = "testadmin"
FORGEJO_ADMIN_PASSWORD = "testpass123!"
FORGEJO_ADMIN_EMAIL = "admin@test.nil"
FORGEJO_INTERNAL_PORT = 3000


def _skip_or_fail(msg: str, *, strict: bool) -> None:
    """``pytest.fail`` when *strict* (``--forgejo``), else ``pytest.skip``."""
    if strict:
        pytest.fail(msg)
    else:
        pytest.skip(msg)


def _find_container_runtime(*, strict: bool) -> str:
    """Return 'podman' or 'docker', or skip/fail if unavailable.

    This is the only place that may legitimately *skip* — all other
    failures (pull, start, etc.) are always fatal because they indicate
    real operational problems, not a missing prerequisite.
    """
    if explicit := os.environ.get("DATALAD_TESTS_CONTAINER_RUNTIME"):
        if shutil.which(explicit) is None:
            _skip_or_fail(
                f"DATALAD_TESTS_CONTAINER_RUNTIME={explicit!r} not found",
                strict=strict,
            )
        return explicit
    for rt in ("podman", "docker"):
        if shutil.which(rt):
            return rt
    _skip_or_fail(
        "No container runtime (podman/docker) available",
        strict=strict,
    )
    return ""  # unreachable, keeps type checkers happy


def _run(
    runtime: str, *args: str, check: bool = True
) -> subprocess.CompletedProcess:  # type: ignore[type-arg]
    cmd = [runtime, *args]
    lgr.debug("Container: %s", " ".join(cmd))
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def _container_is_running(runtime: str, name: str) -> bool:
    r = _run(runtime, "inspect", "--format", "{{.State.Running}}", name, check=False)
    return r.returncode == 0 and "true" in r.stdout.lower()


def _get_host_port(runtime: str, container_id: str) -> int:
    """Extract the host port mapped to the container's internal port."""
    r = _run(runtime, "port", container_id, str(FORGEJO_INTERNAL_PORT))
    # Output like "0.0.0.0:12345" or "0.0.0.0:12345\n:::12345"
    for line in r.stdout.strip().splitlines():
        if ":" in line:
            return int(line.rsplit(":", 1)[1])
    raise RuntimeError(f"Could not determine host port: {r.stdout!r}")


def _wait_for_forgejo(url: str, timeout: int = 60) -> None:
    """Poll the Forgejo API until it responds or *timeout* seconds elapse."""
    api_url = f"{url}/api/forgejo/v1/version"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if requests.get(api_url, timeout=5).status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(2)
    raise RuntimeError(f"Forgejo at {url} not ready within {timeout}s")


def _create_admin(runtime: str, container_id: str) -> None:
    _run(
        runtime,
        "exec",
        container_id,
        "forgejo",
        "admin",
        "user",
        "create",
        "--admin",
        "--username",
        FORGEJO_ADMIN_USER,
        "--password",
        FORGEJO_ADMIN_PASSWORD,
        "--email",
        FORGEJO_ADMIN_EMAIL,
        "--must-change-password=false",
    )


def _create_api_token(url: str) -> str:
    name = f"test-{uuid_mod.uuid4().hex[:8]}"
    resp = requests.post(
        f"{url}/api/v1/users/{FORGEJO_ADMIN_USER}/tokens",
        auth=(FORGEJO_ADMIN_USER, FORGEJO_ADMIN_PASSWORD),
        json={"name": name, "scopes": ["all"]},
    )
    resp.raise_for_status()
    return resp.json()["sha1"]


@dataclass
class ForgejoInstance:
    url: str
    admin_user: str
    admin_password: str
    api_token: str
    runtime: str
    container_id: str


@pytest.fixture(scope="session")
def forgejo_instance(request: pytest.FixtureRequest) -> Iterator[ForgejoInstance]:
    """Start an ephemeral Forgejo-aneksajo container for testing.

    Only skips when no container runtime is found (and ``--forgejo`` is
    not given).  All other failures (pull, start, etc.) are always fatal.

    Environment variables:

    * ``DATALAD_TESTS_CONTAINER_RUNTIME`` — force ``podman`` or ``docker``
    * ``DATALAD_TESTS_CONTAINER_PERSIST`` — keep container across runs
    * ``DATALAD_TESTS_CONTAINER_PULL``  — set to ``0`` to skip image pull
    """
    no_forgejo = request.config.getoption("--no-forgejo", default=False)
    no_network = request.config.getoption("--no-network", default=False)
    # --no-forgejo: skip instead of fail; default is strict (fail on errors)
    strict = not no_forgejo
    network = not no_network
    runtime = _find_container_runtime(strict=strict)
    persist = os.environ.get("DATALAD_TESTS_CONTAINER_PERSIST")
    do_pull = os.environ.get("DATALAD_TESTS_CONTAINER_PULL", "1") != "0"
    # Use a PID-suffixed name to avoid collisions between concurrent runs.
    # Persistent containers keep the fixed name so they can be reused.
    container_name = (
        FORGEJO_CONTAINER_NAME if persist else f"{FORGEJO_CONTAINER_NAME}-{os.getpid()}"
    )

    reused = False
    container_id = ""

    if persist and _container_is_running(runtime, container_name):
        lgr.info("Reusing persisted Forgejo container %s", container_name)
        container_id = _run(
            runtime,
            "inspect",
            "--format",
            "{{.Id}}",
            container_name,
        ).stdout.strip()
        reused = True
    else:
        # Remove a stale (stopped) container with the same name, but
        # never kill a running one — it may belong to another test session.
        if not _container_is_running(runtime, container_name):
            _run(runtime, "rm", "-f", container_name, check=False)

        if do_pull and network:
            lgr.info("Pulling Forgejo-aneksajo image …")
            r = _run(runtime, "pull", FORGEJO_IMAGE, check=False)
            if r.returncode:
                pytest.fail(f"Failed to pull {FORGEJO_IMAGE}: {r.stderr}")
        elif do_pull and not network:
            # No --network: check if the image is already available locally.
            r = _run(runtime, "image", "exists", FORGEJO_IMAGE, check=False)
            if r.returncode:
                _skip_or_fail(
                    f"{FORGEJO_IMAGE} not available locally and "
                    f"--network not given (cannot pull)",
                    strict=strict,
                )

        run_args = [
            "run",
            "-d",
            "--name",
            container_name,
            "-p",
            str(FORGEJO_INTERNAL_PORT),
            "-e",
            "FORGEJO__security__INSTALL_LOCK=true",
        ]
        if not persist:
            run_args.insert(1, "--rm")
        run_args.append(FORGEJO_IMAGE)

        r = _run(runtime, *run_args, check=False)
        if r.returncode:
            pytest.fail(f"Failed to start Forgejo container: {r.stderr}")
        container_id = r.stdout.strip()

    try:
        port = _get_host_port(runtime, container_id)
        url = f"http://127.0.0.1:{port}"  # noqa: E231
        _wait_for_forgejo(url)

        if not reused:
            _create_admin(runtime, container_id)

        yield ForgejoInstance(
            url=url,
            admin_user=FORGEJO_ADMIN_USER,
            admin_password=FORGEJO_ADMIN_PASSWORD,
            api_token=_create_api_token(url),
            runtime=runtime,
            container_id=container_id,
        )
    finally:
        if not persist:
            lgr.info("Stopping Forgejo container %s", container_id)
            _run(runtime, "stop", container_id, check=False)
            _run(runtime, "rm", "-f", container_name, check=False)


@dataclass
class ForgejoRepo:
    local_path: Path
    remote_url: str  # URL with correct host port (not FORGEJO_INTERNAL_PORT)
    relpath: str
    annex_key: str
    content: bytes
    repo_name: str
    instance: ForgejoInstance


def _fix_clone_url(api_clone_url: str, instance_url: str) -> str:
    """Replace host:port in the API-returned clone URL with the real one.

    Forgejo inside the container returns ``http://localhost:FORGEJO_INTERNAL_PORT/…``
    but the container's FORGEJO_INTERNAL_PORT is mapped to a dynamic host port.
    """
    return re.sub(
        r"https?://[^/]+",
        instance_url.rstrip("/"),
        api_clone_url,
        count=1,
    )


@pytest.fixture
def forgejo_repo(
    forgejo_instance: ForgejoInstance,
    tmp_home: Path,  # noqa: U100
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[ForgejoRepo]:
    """Create a local dataset, push it to the Forgejo instance.

    Uses the same ``Dataset.create()`` / ``.save()`` / ``.push()`` pattern
    as the other dataset fixtures in ``conftest.py``.
    """
    repo_name = f"test-annex-{uuid_mod.uuid4().hex[:8]}"

    # Create an empty repo on Forgejo (no auto_init — we push our content)
    resp = requests.post(
        f"{forgejo_instance.url}/api/v1/user/repos",
        headers={"Authorization": f"token {forgejo_instance.api_token}"},
        json={"name": repo_name, "auto_init": False, "default_branch": "main"},
    )
    resp.raise_for_status()

    remote_url = _fix_clone_url(
        resp.json()["clone_url"],
        forgejo_instance.url,
    )
    # Put the token in the username field with an empty password.
    # Forgejo accepts API tokens as usernames, and this avoids a
    # git-annex bug where it sends "..." instead of the actual password
    # from the URL.  See https://git-annex.branchable.com/bugs/...
    auth_url = remote_url.replace(
        "://",
        f"://{forgejo_instance.api_token}:@",  # noqa: E231
    )

    local_path = tmp_path_factory.mktemp("forgejo_repo") / repo_name
    test_content = b"Hello from datalad-fuse forgejo test!\n" * 100
    test_file = "testfile.bin"

    try:
        # Create dataset locally — same pattern as url_dataset / big_url_dataset
        ds = Dataset(local_path).create()
        (local_path / test_file).write_bytes(test_content)
        ds.save(message="Add test file")

        annex_key = ds.repo.get_file_annexinfo(test_file)["key"]

        # Allow git-annex to connect to localhost (needed to fetch the
        # remote's .git/config and discover its annex UUID).
        ds.config.set(
            "annex.security.allowed-ip-addresses",
            "127.0.0.1",
            scope="local",
        )

        # Use the auth URL as the remote URL so git-annex can
        # authenticate when probing the remote's .git/config during
        # `git annex init`.  Aneksajo lazily initialises the repo-side
        # annex (setting annex.uuid) on the first authenticated request
        # to /config — see forgejo-aneksajo#113.
        ds.repo.add_remote("forgejo", auth_url)

        for iter in range(4):
            ds.repo.call_git(
                ["push", "forgejo", ds.repo.get_active_branch(), "git-annex"]
            )
            ds.repo.call_git(["fetch", "forgejo"])
            ds.repo.call_git(["annex", "init"])
            ds.config.reload()
            for cfg in "annex-ignore", "annex-ignore-auto":
                cfg_ = f"remote.forgejo.{cfg}"
                if ds.config.get(cfg_):
                    ds.config.unset(cfg_, scope="local")
            if ds.config.get("remote.forgejo.annex-uuid"):
                lgr.info("annex-uuid discovered on iter %d", iter)
                break
            lgr.info("iter %d: annex-uuid not yet set", iter)
            sleep(0.5)

        # Verify annex content was actually transferred
        assert ds.config.get("remote.forgejo.annex-uuid"), (
            "remote.forgejo.annex-uuid not set after push — "
            "git-annex could not discover the remote"
        )
        # git-annex auto-discovers annexurl from the remote but uses
        # the internal container port (localhost:3000) instead of the
        # mapped host port.  Fix it up.
        # https://git-annex.branchable.com/bugs/annex_overwrites_existing_p2p_annexurl/
        correct_annexurl = f"annex+{forgejo_instance.url}/git-annex-p2phttp"
        ds.repo.call_git(
            [
                "config",
                "remote.forgejo.annexurl",
                correct_annexurl,
            ]
        )

        assert not ds.config.get("remote.forgejo.annex-ignore"), (
            "remote.forgejo.annex-ignore is set — " "git-annex is ignoring this remote"
        )

        # Do push our data file
        ds.push(to="forgejo", data="anything")

        yield ForgejoRepo(
            local_path=local_path,
            remote_url=remote_url,
            relpath=test_file,
            annex_key=annex_key,
            content=test_content,
            repo_name=repo_name,
            instance=forgejo_instance,
        )
    finally:
        if not os.environ.get("DATALAD_TESTS_CONTAINER_PERSIST"):
            requests.delete(
                f"{forgejo_instance.url}/api/v1/repos/"
                f"{forgejo_instance.admin_user}/{repo_name}",
                headers={
                    "Authorization": f"token {forgejo_instance.api_token}",
                },
            )

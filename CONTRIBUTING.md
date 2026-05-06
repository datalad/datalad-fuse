# Contributing to DataLad FUSE

## Running Tests

### Basic tests

```bash
tox -e py3
```

### FUSE mount tests

Requires FUSE system libraries (`apt-get install fuse` on Debian/Ubuntu):

```bash
tox -e py3 -- --libfuse
```

### Forgejo-aneksajo integration tests

These tests start an ephemeral [Forgejo-aneksajo](https://codeberg.org/forgejo-aneksajo/forgejo-aneksajo)
container, create a repository with annexed content, and verify that
datalad-fuse can transparently access files via the `annex/objects`
HTTP endpoint.

**Requirements**: `podman` or `docker` must be available.  Without
`--forgejo`, tests auto-skip when the container cannot start.  With
`--forgejo`, failures are fatal so you see exactly what went wrong.

```bash
# Run forgejo tests, fail loudly on container problems
tox -e py3 -- --forgejo -k forgejo

# Run all tests (forgejo tests auto-skip if container unavailable)
tox -e py3
```

#### Environment variables

| Variable                           | Default          | Description                                                              |
|------------------------------------|------------------|--------------------------------------------------------------------------|
| `DATALAD_TESTS_FORGEJO_URL`        | *(unset)*        | Run against this externally-managed Forgejo-aneksajo URL (no container)  |
| `DATALAD_TESTS_FORGEJO_TOKEN`      | *(unset)*        | API token for the external instance; required when `…_URL` is set        |
| `DATALAD_TESTS_CONTAINER_RUNTIME`  | *(auto-detect)*  | Force `podman` or `docker`                                               |
| `DATALAD_TESTS_CONTAINER_PERSIST`  | *(unset)*        | Keep container running across test runs for faster iteration            |
| `DATALAD_TESTS_CONTAINER_PULL`     | `1`              | Set to `0` to skip pulling the container image                           |

#### Running against an external Forgejo-aneksajo instance

To validate the test suite against an existing deployment (e.g.
`hub.datalad.org`) instead of booting a local container, set:

```bash
export DATALAD_TESTS_FORGEJO_URL=https://hub.datalad.org
export DATALAD_TESTS_FORGEJO_TOKEN=<api-token>     # write:repository scope
tox -e py3 -- -k forgejo
```

The token's user account will own the test repos; each test repo has a
random name (`test-annex-XXXXXXXX`) and is deleted on teardown.

#### Container image

The tests use:
```
codeberg.org/forgejo-aneksajo/forgejo-aneksajo:forgejo-rootless
```

When `DATALAD_TESTS_CONTAINER_PERSIST` is set, the container is named
`datalad-fuse-test-forgejo` and will be reused on subsequent runs.
To stop a persisted container manually:

```bash
podman stop datalad-fuse-test-forgejo
podman rm datalad-fuse-test-forgejo
```

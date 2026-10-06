# Contributing to DataLad FUSE

## Documentation

The documentation at https://datalad-fuse.readthedocs.io is built with
[Sphinx](https://www.sphinx-doc.org) from `docs/source/`; the API reference
is generated from the docstrings, and the command line reference from the
commands' parameter definitions.  To build it locally:

```bash
pip install -e . -r docs/requirements.txt
make -C docs html
# then open docs/build/html/index.html
```

Warnings are treated as errors.  The documentation is also built for every
pull request, both by the `docs` GitHub workflow and by Read the Docs, which
links a preview of the rendered documentation from the pull request's checks.

## Running Tests

### Basic tests

```bash
tox -e py3
```

### FUSE mount tests

Requires FUSE system libraries (e.g. `apt-get install fuse3 libfuse2t64` on
Ubuntu 24.04; see the installation instructions in the documentation):

```bash
tox -e py3 -- --libfuse
```

### Forgejo-aneksajo integration tests

These tests start an ephemeral [Forgejo-aneksajo](https://codeberg.org/forgejo-aneksajo/forgejo-aneksajo)
container, create a repository with annexed content, and verify that
datalad-fuse can transparently access files via the `annex/objects`
HTTP endpoint.

**Requirements**: `podman` or `docker` must be available.  By default,
failing to start the container is an error, so you see exactly what went
wrong.  With `--no-forgejo`, the forgejo tests are skipped instead.

```bash
# Run forgejo tests, fail loudly on container problems
tox -e py3 -- -k forgejo

# Run all tests, skipping the forgejo tests
tox -e py3 -- --no-forgejo
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
codeberg.org/forgejo-aneksajo/forgejo-aneksajo:v14.0.3-git-annex2-rootless
```

When `DATALAD_TESTS_CONTAINER_PERSIST` is set, the container is named
`datalad-fuse-test-forgejo` and will be reused on subsequent runs.
To stop a persisted container manually:

```bash
podman stop datalad-fuse-test-forgejo
podman rm datalad-fuse-test-forgejo
```

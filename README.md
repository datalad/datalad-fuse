# DataLad FUSE extension package

[![codecov.io](https://codecov.io/github/datalad/datalad-fuse/coverage.svg?branch=master)](https://codecov.io/github/datalad/datalad-fuse?branch=master) [![tests](https://github.com/datalad/datalad-fuse/workflows/Test/badge.svg)](https://github.com/datalad/datalad-fuse/actions?query=workflow%3ATest) [![docs](https://github.com/datalad/datalad-fuse/workflows/docs/badge.svg)](https://github.com/datalad/datalad-fuse/actions?query=workflow%3Adocs)

`datalad-fuse` gives read access to the files of a
[DataLad](https://datalad.org) dataset, or of any
[git-annex](https://git-annex.branchable.com) repository, without first
fetching their content with `datalad get` or `git annex get`.  When an annexed
file has no content locally, the URLs that git-annex knows for it are used to
read only the parts of the file that are actually needed, via
[fsspec](https://github.com/fsspec/filesystem_spec), optionally caching them on
local disk.

The same machinery is available at three levels:

- a **FUSE mount** (`datalad fusefs`), so that any program, not only Python,
  can open the files as if their content were present;
- **Python objects** (`FsspecAdapter` and `DatasetAdapter` in
  `datalad_fuse.fsspec`) that return a file object for any file of a dataset,
  with no FUSE involved;
- **DataLad commands** (`fusefs`, `fsspec-head`, `fsspec-cache-clear`),
  available from the command line and as `datalad.api` functions.

## Installation

`datalad-fuse` requires Python 3.9 or higher and, like DataLad itself,
[git-annex](https://git-annex.branchable.com/install/).  Install it with
[pip](https://pip.pypa.io):

    python3 -m pip install datalad-fuse

If git-annex is not installed system-wide, `python3 -m pip install git-annex`
provides it within the Python environment.

The `datalad fusefs` command additionally needs FUSE (the
[fusepy](https://github.com/fusepy/fusepy) bindings use `libfuse.so.2`); on
Debian-based systems it can be installed with:

    sudo apt-get install fuse

Access from Python via `FsspecAdapter`/`DatasetAdapter`, and `datalad
fsspec-head`, do not need FUSE.

## How it works

For each file it is asked to open, `datalad-fuse` determines which
(sub)dataset the file belongs to and what git-annex knows about it:

- files not under annex, and annexed files whose content is present locally,
  are opened directly from disk;
- for an annexed file without local content, candidate URLs are tried in turn
  until one of them opens:
  1. http(s) URLs that git-annex has recorded for the file's key (as listed by
     `git annex whereis`), e.g. those of the `web` special remote;
  2. `annex/objects/...` locations on http(s) git remotes that have the key,
     including [Forgejo-aneksajo](https://codeberg.org/forgejo-aneksajo/forgejo-aneksajo)
     instances;
  3. as a fallback, S3 special remotes configured with `exporttree=yes` and a
     `publicurl`; the matching object version is picked by the size recorded
     in the annex key.

Remote files are read with HTTP range requests, so opening a large file and
reading its header transfers only a few megabytes.  With caching enabled
(`caching=True` in Python, `--caching ondisk` on the command line), the fetched
blocks are kept in a sparse cache under `.git/datalad/cache/fsspec/` of each
dataset, which `datalad fsspec-cache-clear` removes.  Access is read-only:
nothing is added to the annex; use `datalad get` to keep a file's content
locally.

## Example: streaming NWB data from a DANDI dandiset

Public dandisets of the [DANDI Archive](https://dandiarchive.org) are
mirrored as git-annex repositories under
[github.com/dandisets](https://github.com/dandisets), with the S3 and DANDI API
URLs of the asset files registered in git-annex, which is all that
`datalad-fuse` needs.  The examples below follow the DANDI tutorial
[Streaming and interacting with NWB data from DANDI](https://docs.dandiarchive.org/example-notebooks/tutorials/bcm_2024/analysis-demo/),
which uses position tracking and sorted units from rat medial entorhinal cortex
([Dandiset 000582](https://dandiarchive.org/dandiset/000582); Sargolini et al.,
2006).

Cloning fetches only git history and metadata, no NWB content:

    datalad clone https://github.com/dandisets/000582
    # or just: git clone https://github.com/dandisets/000582

Published versions of a dandiset are git tags (e.g. `0.251111.2151`), so
`git -C 000582 checkout 0.251111.2151` pins an analysis to the exact versions of
the files in that release.

### From Python, without FUSE

Python requirements for this example: `pip install datalad-fuse h5py pynwb`.

```python
from contextlib import closing

import h5py
import pynwb

from datalad_fuse.fsspec import DatasetAdapter

nwb_path = "sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb"

with closing(DatasetAdapter("000582", caching=True)) as dsa:
    print(dsa.get_file_state(nwb_path))
    # (<FileState.NO_CONTENT: 2>, AnnexKey(backend='SHA256E', name='43b3b4...', size=15657857, ...))
    with dsa.open(nwb_path) as f, h5py.File(f, "r") as h5:
        with pynwb.NWBHDF5IO(file=h5) as io:
            nwbfile = io.read()
            print(nwbfile.subject.species)  # Rattus norvegicus
            position = nwbfile.processing["behavior"]["Position"]["SpatialSeriesLED1"]
            x = position.data[:, 0]  # data are read lazily, on access
            ts = position.timestamps[:]
            units_df = nwbfile.units.to_dataframe()
```

Unlike in the tutorial, there is no need to look up the asset's S3 URL via the
DANDI API: it comes from git-annex, for the version of the file in the commit
that is checked out.  From `nwbfile` on, the rest of the tutorial (plots,
[pynapple](https://pynapple.org) analyses, ...) applies unchanged, as long as
data are read while the file is still open.

### Through a FUSE mount

A FUSE mount makes the same files available to code and tools that expect
local paths:

    mkdir -p mnt
    datalad fusefs -d 000582 --foreground mnt &
    until mountpoint -q mnt; do sleep 0.1; done
    python -c '
    import pynwb
    with pynwb.NWBHDF5IO("mnt/sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb") as io:
        print(io.read().units.to_dataframe())
    '
    fusermount -u mnt  # unmount; datalad fusefs then exits

From Python, `datalad.api.fusefs` blocks for as long as the file system is
mounted, so run it in a separate process:

```python
from multiprocessing import Process
import os
import subprocess
import time

from datalad.api import fusefs

os.makedirs("mnt", exist_ok=True)
mount = Process(
    target=fusefs,
    args=("mnt",),
    kwargs={"dataset": "000582", "foreground": True, "caching": "ondisk"},
)
mount.start()
while mount.is_alive() and not os.path.ismount("mnt"):
    time.sleep(0.1)
try:
    ...  # work with files under mnt/
finally:
    subprocess.run(["fusermount", "-u", "mnt"], check=True)
    mount.join()
```

## Python API

### DataLad commands

All commands below are available as functions in `datalad.api` and as methods
of `datalad.api.Dataset`, with the same parameters as on the command line:

```python
from datalad.api import Dataset

ds = Dataset("000582")
res = ds.fsspec_head(nwb_path, bytes=8, result_renderer="disabled")
res[0]["data"]  # b'\x89HDF\r\n\x1a\n'
ds.fsspec_cache_clear()
ds.fusefs("mnt", foreground=True)  # blocks until unmounted
```

### `datalad_fuse.fsspec.FsspecAdapter(root, caching, mode_transparent=False)`

Opens files anywhere in a hierarchy of datasets rooted at `root`: each path is
mapped to the (installed) subdataset that contains it, and a `DatasetAdapter`
is created for that subdataset on first use.  Use it as a context manager, so
that the `git annex` processes it starts are stopped on exit.  Paths passed to
its methods must lie under `root` and be spelled consistently with it (e.g.
both absolute).

- `open(path, mode="rb", encoding="utf-8", errors=None)` returns a read-only
  binary (`"rb"`) or text (`"r"`, `"rt"`) file object;
- `get_file_state(path)` returns a `(FileState, AnnexKey | None)` tuple, where
  `FileState` is one of `NOT_ANNEXED`, `NO_CONTENT` or `HAS_CONTENT`;
- `is_under_annex(path)` tells whether a file is annexed;
- `get_commit_datetime(path)` returns the date of the `HEAD` commit of the
  dataset containing `path`;
- `resolve_dataset(path)` returns the `(DatasetAdapter, relative_path)` pair
  used for `path`.

```python
from pathlib import Path

from datalad_fuse.fsspec import FsspecAdapter

root = Path("000582").resolve()
with FsspecAdapter(root, caching=False) as fsa:
    with fsa.open(root / "dandiset.yaml", "rt") as f:  # not annexed: read from disk
        print(f.readline())
    with fsa.open(root / nwb_path) as f:  # annexed, no content: streamed
        print(f.read(8))  # b'\x89HDF\r\n\x1a\n'
```

### `datalad_fuse.fsspec.DatasetAdapter(path, caching, mode_transparent=False)`

The same for a single dataset, with paths relative to its top directory
(`open()` and `get_file_state()` as above).  It is not a context manager:
call `close()` when done, or wrap it in `contextlib.closing()` as in the
example above.  In addition, it provides

- `get_urls(key)`, which yields the candidate URLs for an annex key;
- `clear()`, which removes the on-disk cache of the dataset;
- attributes `path`, `annex` (DataLad's `AnnexRepo`, or `None` for a
  dataset without an annex), `commit_dt` and `fs` (the underlying fsspec file
  system).

```python
with closing(DatasetAdapter("000582", caching=False)) as dsa:
    fstate, key = dsa.get_file_state(nwb_path)
    print(list(dsa.get_urls(str(key))))
    # ['https://api.dandiarchive.org/api/assets/2b9e441b-.../download/',
    #  'https://dandiarchive.s3.amazonaws.com/blobs/26a/22c/26a22c31-...?versionId=...']
```

With `mode_transparent=True`, both adapters also open files under `.git/`,
such as `.git/annex/objects/...` paths that annexed symlinks point to.

### `datalad_fuse.utils.AnnexKey`

A dataclass for parsing and formatting
[git-annex keys](https://git-annex.branchable.com/internals/key_format/):

```python
from datalad_fuse.utils import AnnexKey

key = "SHA256E-s15657857--43b3b435b953d22e276acc494af2926b63deaf15d2834531b2a87d08a8458a09.nwb"
k = AnnexKey.parse(key)
k.backend, k.size, k.suffix  # ('SHA256E', 15657857, '.nwb')
assert str(k) == key
```

`AnnexKey.parse_filename()` does the same for key file names as found under
`.git/annex/objects/`, which escape some characters.

### `datalad_fuse.fuse_.DataLadFUSE(root, caching, mode_transparent=False)`

The [fusepy](https://github.com/fusepy/fusepy) `Operations` class behind
`datalad fusefs`, built on `FsspecAdapter`.  Mount it directly to pass other
FUSE options, which fusepy takes as keyword arguments.  `root` must be an
absolute path:

```python
import os

from fuse import FUSE

from datalad_fuse.fuse_ import DataLadFUSE

FUSE(
    DataLadFUSE(os.path.abspath("000582"), caching=True),
    "mnt",
    foreground=True,
    ro=True,
)
```

## Commands

### `datalad fsspec-cache-clear [<options>]`

Clears the local download cache for a dataset.

#### Options

- `-d <DATASET>`, `--dataset <DATASET>` — Specify the dataset to operate on.
  If no dataset is given, an attempt is made to identify the dataset based on
  the current working directory.

- `-r`, `--recursive` — Clear the caches of subdatasets as well.

### `datalad fsspec-head [<options>] <path>`

Shows leading lines/bytes of an annexed file by fetching its data from a remote
URL.

#### Options

- `-d <DATASET>`, `--dataset <DATASET>` — Specify the dataset to operate on.
  If no dataset is given, an attempt is made to identify the dataset based on
  the current working directory.

- `-n <INT>`, `--lines <INT>` — How many lines to show (default: 10)

- `-c <INT>`, `--bytes <INT>` — How many bytes to show

- `--caching {none,ondisk}` — Whether to cache the fetched data on disk
  (default: `none`)

- `--mode-transparent` — Support reading files under the dataset's `.git`
  directory

### `datalad fusefs [<options>] <mount-path>`

Create a read-only FUSE mount at `<mount-path>` that exposes the files in the
given dataset.  Opening a file under the mount that is not locally present in
the dataset will cause its contents to be downloaded from the file's web URL as
needed.

When the command finishes, `fsspec-cache-clear` may be run depending on the
value of the `datalad.fusefs.cache-clear` configuration option.  If it is set
to "`visited`", then any (sub)datasets that were accessed in the FUSE mount
will have their caches cleared; if it is instead set to "`recursive`", then all
(sub)datasets in the dataset being operated on will have their caches cleared.

#### Options

- `--allow-other` — Allow all users to access files in the mount.  This
  requires setting `user_allow_other` in `/etc/fuse.conf`.

- `--caching {none,ondisk}` — Whether to cache the fetched data on disk
  (default: `none`)

- `-d <DATASET>`, `--dataset <DATASET>` — Specify the dataset to operate on.
  If no dataset is given, an attempt is made to identify the dataset based on
  the current working directory.

- `-f`, `--foreground` — Run the FUSE process in the foreground; use Ctrl-C to
  exit.  This option is currently required.

- `--mode-transparent` — Expose the dataset's `.git` directory in the mount

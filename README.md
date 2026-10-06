# DataLad FUSE extension package

[![codecov.io](https://codecov.io/github/datalad/datalad-fuse/coverage.svg?branch=master)](https://codecov.io/github/datalad/datalad-fuse?branch=master) [![tests](https://github.com/datalad/datalad-fuse/workflows/Test/badge.svg)](https://github.com/datalad/datalad-fuse/actions?query=workflow%3ATest) [![docs](https://readthedocs.org/projects/datalad-fuse/badge/?version=latest)](https://datalad-fuse.readthedocs.io/en/latest/)

`datalad-fuse` lets you read files of [DataLad](https://www.datalad.org)
datasets and [git-annex](https://git-annex.branchable.com) repositories
without downloading them first: only the parts of files that are actually
read are fetched, via [fsspec](https://filesystem-spec.readthedocs.io), from
the URLs that git-annex knows.  Use it through a FUSE mount, so that any
program can open the files, or directly from Python.

**Documentation: https://datalad-fuse.readthedocs.io**

## Installation

    python3 -m pip install datalad-fuse

[git-annex](https://git-annex.branchable.com/install/) is required, and FUSE
for mounting datasets (`sudo apt-get install fuse` on Debian/Ubuntu).  See
the [installation instructions](https://datalad-fuse.readthedocs.io/en/latest/installation.html)
for details.

## Example

Clone a dataset, here [Dandiset 000582](https://dandiarchive.org/dandiset/000582)
from the DANDI Archive.  This gets all file names, but none of the 1.86 GB of
file content:

    datalad clone https://github.com/dandisets/000582

On the command line, mount the dataset and use any tool on its files:

    mkdir mnt
    datalad fusefs -d 000582 --foreground mnt &
    h5ls mnt/sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb
    fusermount -u mnt

In Python, open files directly, without FUSE:

```python
from contextlib import closing

import h5py
import pynwb

from datalad_fuse.fsspec import DatasetAdapter

with closing(DatasetAdapter("000582", caching=False)) as dsa:
    with dsa.open("sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb") as f:
        with h5py.File(f, "r") as h5, pynwb.NWBHDF5IO(file=h5) as io:
            print(io.read().units.to_dataframe())
```

The [tutorial](https://datalad-fuse.readthedocs.io/en/latest/tutorial.html)
continues this example, and the documentation covers the
[command line](https://datalad-fuse.readthedocs.io/en/latest/cli.html) and
[Python](https://datalad-fuse.readthedocs.io/en/latest/python.html)
interfaces in detail.

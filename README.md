# DataLad FUSE extension package

[![codecov.io](https://codecov.io/github/datalad/datalad-fuse/coverage.svg?branch=master)](https://codecov.io/github/datalad/datalad-fuse?branch=master) [![tests](https://github.com/datalad/datalad-fuse/workflows/Test/badge.svg)](https://github.com/datalad/datalad-fuse/actions?query=workflow%3ATest) [![docs](https://github.com/datalad/datalad-fuse/workflows/docs/badge.svg)](https://github.com/datalad/datalad-fuse/actions?query=workflow%3Adocs)

`datalad-fuse` provides commands for reading files in a
[DataLad](http://datalad.org) dataset from their remote web URLs without having
to download them in their entirety first.  Instead,
[fsspec](http://github.com/fsspec/filesystem_spec) is used to sparsely download
and locally cache the files as needed.

## Installation

Current version of `datalad-fuse` requires Python 3.7 or higher.  Just use
[pip](https://pip.pypa.io) for Python 3 (You have pip, right?) to install it:

    python3 -m pip install datalad-fuse

In addition, use of the `datalad fusefs` command requires FUSE to be installed;
on Debian-based systems, this can be done with:

    sudo apt-get install fuse

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

- `-d <DATASET>`, `--dataset <DATASET>` — Specify the dataset to operate on.
  If no dataset is given, an attempt is made to identify the dataset based on
  the current working directory.

- `-f`, `--foreground` — Run the FUSE process in the foreground; use Ctrl-C to
  exit.  This option is currently required.

- `--mode-transparent` — Expose the dataset's `.git` directory in the mount

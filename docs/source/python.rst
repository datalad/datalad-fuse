Python usage
************

From Python, files of a dataset can be opened without any FUSE mount, as
file objects that many libraries accept in place of a file name.  This page
covers:

- `~datalad_fuse.fsspec.DatasetAdapter`: open files of a single dataset;
- `~datalad_fuse.fsspec.FsspecAdapter`: open files across a dataset and its
  subdatasets;
- the ``datalad`` commands of ``datalad-fuse``, called from Python;
- mounting a dataset with FUSE from Python.

The examples use the dandiset cloned in the :doc:`tutorial`, and are run from
the directory containing it:

.. code-block:: python

   nwb_path = "sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb"


Opening files of a dataset
==========================

`~datalad_fuse.fsspec.DatasetAdapter` takes the path to a dataset (or any
git-annex repository), and opens files by their path relative to the
dataset's top directory:

.. code-block:: python

   from contextlib import closing

   from datalad_fuse.fsspec import DatasetAdapter

   with closing(DatasetAdapter("000582", caching=False)) as dsa:
       with dsa.open(nwb_path) as f:  # binary mode, like open(..., "rb")
           print(f.read(8))  # b'\x89HDF\r\n\x1a\n'
       with dsa.open("dandiset.yaml", "rt") as f:  # text mode
           print(f.readline())

.. important::
   Many libraries (h5py, PyNWB, zarr, ...) read data *lazily*, only when you
   access them.  The file object must stay open until all data you need have
   been read, so do the reading inside the ``with`` blocks, or keep the
   objects open while working interactively (see the :doc:`tutorial`).
   Reading after the file was closed fails, with h5py with an error
   mentioning "identifier is not of specified type".

``open()`` returns a seekable, read-only file object:

- for files read from disk (not annexed, or with content present), a regular
  Python file object;
- for files read from a URL, an fsspec file object, which fetches data as it
  is read.

Text mode (``"r"`` or ``"rt"``) accepts ``encoding`` (default ``"utf-8"``) and
``errors`` arguments, as the built-in :func:`open` does.  Writing is not
supported.

The ``caching`` argument is required: ``True`` keeps fetched data in an
on-disk cache inside the dataset, ``False`` only buffers them in memory while
a file is open (see :ref:`caching`).  ``dsa.clear()`` removes the dataset's
cache.

The adapter starts ``git annex`` processes to answer its queries;
``close()``, called by :func:`contextlib.closing` above, stops them.

Passing file objects to other libraries
---------------------------------------

Anything that reads from a Python file object can read from these.  For
example:

.. code-block:: python

   import json

   import h5py
   import pandas as pd

   with closing(DatasetAdapter("path/to/dataset", caching=True)) as dsa:
       # HDF5, and formats based on it such as NWB
       with dsa.open("data/recording.h5") as f, h5py.File(f, "r") as h5:
           signal = h5["signal"][:1000]
       # tabular text data
       with dsa.open("participants.tsv", "rt") as f:
           participants = pd.read_csv(f, sep="\t")
       # JSON
       with dsa.open("dataset_description.json", "rt") as f:
           description = json.load(f)

Libraries that need a file *name* rather than a file object cannot use these
objects; use a FUSE mount for them (see :ref:`python-mount`).

Inspecting files
----------------

`~datalad_fuse.fsspec.DatasetAdapter.get_file_state` tells whether a file is
annexed and whether its content is present, and returns its git-annex key as
an `~datalad_fuse.utils.AnnexKey`:

.. code-block:: python

   with closing(DatasetAdapter("000582", caching=False)) as dsa:
       state, key = dsa.get_file_state(nwb_path)
       print(state)  # FileState.NO_CONTENT
       print(key.size, key.backend)  # 15657857 SHA256E
       for url in dsa.get_urls(str(key)):
           print(url)

which prints the URLs that will be tried, in order:

.. code-block:: text

   FileState.NO_CONTENT
   15657857 SHA256E
   https://api.dandiarchive.org/api/assets/2b9e441b-56bc-4be2-893e-0e02d22d239d/download/
   https://dandiarchive.s3.amazonaws.com/blobs/26a/22c/26a22c31-09bc-43a4-9187-edc7394ed12c?versionId=__7hm7itizkF8RCsvO.Fidzi7Lqd1OMu

The possible states are described in :doc:`concepts`.
`~datalad_fuse.utils.AnnexKey` can also parse and format keys on its own:

.. code-block:: python

   from datalad_fuse.utils import AnnexKey

   key = AnnexKey.parse("SHA256E-s15657857--43b3b435b953d22e276acc494af2926b63deaf15d2834531b2a87d08a8458a09.nwb")
   print(key.backend, key.size, key.suffix)  # SHA256E 15657857 .nwb
   print(str(key))  # the key again


Datasets with subdatasets
=========================

`~datalad_fuse.fsspec.FsspecAdapter` works on a dataset together with its
installed subdatasets: for each path, it finds the (sub)dataset that contains
it and uses a `~datalad_fuse.fsspec.DatasetAdapter` for that dataset.  It is a
context manager:

.. code-block:: python

   from pathlib import Path

   from datalad_fuse.fsspec import FsspecAdapter

   root = Path("path/to/superdataset").resolve()
   with FsspecAdapter(root, caching=False) as fsa:
       path = root / "subdataset" / "data" / "file.nwb"
       print(fsa.get_file_state(path))
       print(fsa.is_under_annex(path))
       with fsa.open(path) as f:
           header = f.read(1024)

.. important::
   Paths passed to an `~datalad_fuse.fsspec.FsspecAdapter` must be located
   under its ``root`` and spelled the same way.  The simplest is to use
   absolute paths for both, as above.  Paths relative to ``root`` are not
   supported.

Besides ``open()``, ``get_file_state()`` and ``is_under_annex()``, it offers
``get_commit_datetime()`` (the date of the last commit of the dataset
containing a path) and ``resolve_dataset()`` (the
`~datalad_fuse.fsspec.DatasetAdapter` and relative path used for a path).


.. _python-commands:

DataLad commands
================

The commands of ``datalad-fuse`` are available as functions in
``datalad.api`` and as methods of `datalad.api.Dataset`, with the same
options as on the command line (see :doc:`cli`):

.. code-block:: python

   from datalad.api import Dataset

   ds = Dataset("000582")
   res = ds.fsspec_head(nwb_path, bytes=8, result_renderer="disabled")
   print(res[0]["data"])  # b'\x89HDF\r\n\x1a\n'
   ds.fsspec_cache_clear(recursive=True)

Like all DataLad commands, they return result records (dictionaries);
``fsspec_head`` puts the fetched bytes into the ``data`` field of its result.


.. _python-mount:

Mounting from Python
====================

``datalad.api.fusefs`` (or ``Dataset.fusefs``) mounts a dataset, and does not
return until it is unmounted.  To work with the mount from the same Python
program, run it in a separate process:

.. code-block:: python

   from multiprocessing import Process
   import os
   import subprocess
   import time

   from datalad.api import fusefs


   def main():
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
           # any code or tool can now open files under mnt/
           path = "mnt/sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb"
           with open(path, "rb") as f:
               print(f.read(8))
       finally:
           subprocess.run(["fusermount", "-u", "mnt"], check=True)
           mount.join()


   if __name__ == "__main__":  # required by multiprocessing
       main()

Put this code in a script: the ``if __name__ == "__main__"`` guard is required
where :mod:`multiprocessing` starts new processes by re-importing the main
module, which is the default on macOS and, from Python 3.14 on, on Linux.

To pass other FUSE mount options, mount the file system class
`~datalad_fuse.fuse_.DataLadFUSE` directly with fusepy; keyword arguments of
``FUSE()`` become mount options.  ``DataLadFUSE`` needs the *absolute* path
of the dataset:

.. code-block:: python

   import os

   from fuse import FUSE

   from datalad_fuse.fuse_ import DataLadFUSE

   FUSE(
       DataLadFUSE(os.path.abspath("000582"), caching=True),
       "mnt",
       foreground=True,
       ro=True,
   )

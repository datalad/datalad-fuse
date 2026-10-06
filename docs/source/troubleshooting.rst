Troubleshooting
***************

.. _troubleshooting-logging:

Seeing what happens
===================

Debug logging shows which files are opened, their state, and every URL that
is tried:

.. code-block:: console

   $ datalad -l debug fsspec-head -d ds -c 8 path/to/file
   [DEBUG] path/to/file: under annex, does not have content
   [DEBUG] path/to/file: Attempting to open via URL https://...
   ...

``datalad -l debug fusefs ...`` works the same way, but logs every file
system operation.  In Python, enable debug messages for the ``datalad.fuse``
logger after importing DataLad:

.. code-block:: python

   import logging

   import datalad.api  # noqa: F401  (sets up DataLad's logging)

   logging.getLogger("datalad.fuse").setLevel(logging.DEBUG)

``datalad fsspec-head`` is also the quickest way to check whether a
particular file can be read, as it reports errors directly, while programs
reading from a FUSE mount often only report a generic error.


Common problems
===============

"Could not find a usable URL for <path> within <dataset>"
---------------------------------------------------------

The content of the file is not present locally, and none of the candidate
URLs (see :doc:`concepts`) could be opened.  Check what git-annex knows about
the file:

.. code-block:: console

   $ git annex whereis path/to/file

- If no ``http(s)://`` URLs are listed and no listed remote is reachable over
  HTTP(S), ``datalad-fuse`` cannot read the content; use ``datalad get``,
  which can also use SSH remotes and other special remotes.
- If URLs are listed, try one of them with e.g. ``curl -I <URL>``: the server
  may be down, or require authentication, which ``datalad-fuse`` does not
  support.

Errors mentioning "identifier is not of specified type"
-------------------------------------------------------

For example ``RuntimeError: Unable to synchronously get dataspace (identifier
is not of specified type)`` or ``OSError: Can't synchronously read data
(identifier is not of specified type)``, from h5py when reading NWB/HDF5 data.
The data are read lazily, after the file was already closed.  Read the data
while the file is open, inside the ``with`` blocks (see :doc:`python`).

Errors about paths
------------------

- ``AttributeError: 'NoneType' object has no attribute 'get_commit_date'``:
  the path given to `~datalad_fuse.fsspec.DatasetAdapter` is not a dataset.
  Check the path, and the current directory if the path is relative.
- ``FileNotFoundError`` from ``DatasetAdapter.open()`` or ``datalad
  fsspec-head``: paths of files are relative to the top directory of the
  dataset, not to the current directory, so use ``sub-01/file.nwb`` rather
  than ``dataset/sub-01/file.nwb`` or ``file.nwb``.
- ``ValueError`` "Path not under root dataset" or "is not in the subpath of"
  from `~datalad_fuse.fsspec.FsspecAdapter`: use an absolute ``root`` and
  absolute paths (see :doc:`python`).

Reading a file in the mount fails with an odd error
---------------------------------------------------

When the content of a file cannot be fetched, programs reading it from the
mount report a generic error such as "Input/output error", or even
"Numerical result out of range".  Run ``datalad fsspec-head`` on the same file
to see the actual problem, or mount with ``datalad -l debug fusefs ...`` to
see the URLs that are tried.

"Unable to find libfuse" or "fuse: device not found"
----------------------------------------------------

FUSE is not installed, or not available.  Install it (see
:doc:`installation`).  In a container, the container needs access to the
FUSE device, e.g. with Docker::

   docker run --device /dev/fuse --cap-add SYS_ADMIN ...

"Device or resource busy" when unmounting
-----------------------------------------

A program still uses the mount: a shell whose current directory is inside
it, a file manager window, or a program with a file open in it.  Leave the
directory or close the program, and unmount again.  As a last resort,
``fusermount -uz mnt`` detaches the mount immediately, and finishes
unmounting once it is no longer used.

To find mounts you may have forgotten about, run ``findmnt -t fuse`` (or
``mount | grep fuse``); mounts made by ``datalad fusefs`` are listed as
``DataLadFUSE``.

"Transport endpoint is not connected"
-------------------------------------

The ``datalad fusefs`` process ended without unmounting, e.g. because it was
killed.  Unmount the stale mount point, then mount again:

.. code-block:: console

   $ fusermount -u mnt

"Invalid argument" for remote files in the mount
------------------------------------------------

If the path of the dataset given to ``datalad fusefs`` contains a symbolic
link, files whose content is not present cannot be read.  Give the real path
of the dataset instead, e.g. ``datalad fusefs -d "$(realpath path/to/dataset)"
...``.

A subdataset's directory is empty
---------------------------------

The subdataset is not installed.  Install it (without content) with ``datalad
get -n path/to/subdataset``, and remount if you are using a FUSE mount.

Changes to the dataset are not reflected
----------------------------------------

The state of each file (annexed or not, content present or not) is
remembered by an adapter or a mount once determined.
After ``datalad get``, ``datalad drop``, ``git checkout`` etc. in the
dataset, create a new adapter or remount.

Reading is slow
---------------

Reading many small, scattered pieces of a file, or entire files, is slow, as
data are fetched in blocks of 5 MiB (see :doc:`concepts`).  Use ``datalad
get`` for entire files, and caching for files that are read repeatedly.

Warnings "Retrying request to ..."
----------------------------------

A server answered with an error (HTTP 5xx), and the request is retried
automatically, up to four times.  Occasional retries are harmless.

Warning "Destroying fsspecs and collection of N fhs"
----------------------------------------------------

Printed by ``datalad fusefs`` when it unmounts; it is harmless.


.. _limitations:

Known limitations
=================

- Read-only: content is never added to the annex, and the mount does not
  support creating or modifying files.
- Only ``http(s)://`` URLs are used: no SSH remotes, and no ``s3://`` or other
  special-remote protocols, unless git-annex also records an HTTP(S) URL for
  the content.
- No authentication, other than credentials included in a URL, and no use of
  HTTP proxies.
- Fetched content is not checksum-verified.
- Subdatasets are not installed automatically.
- ``datalad fusefs`` must run in the foreground, and FUSE mounts are only
  tested on Linux.

Problems and suggestions are welcome at
https://github.com/datalad/datalad-fuse/issues.

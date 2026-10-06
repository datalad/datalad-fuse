Troubleshooting
***************

.. _troubleshooting-logging:

Seeing what happens
===================

Debug logging shows which files are opened, their state, which backend takes
them and every URL that is tried:

.. code-block:: console

   $ datalad -l debug fsspec-head -d ds -c 8 path/to/file
   [DEBUG] path/to/file: under annex, does not have content
   [DEBUG] path/to/file: opening via backend remfile
   [DEBUG] path/to/file: trying URL https://... (backend=remfile)
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

"Could not open <path> within <dataset> (backends=...)"
-------------------------------------------------------

The content of the file is not present locally, and no backend could open any
of the candidate URLs (see :doc:`concepts`).  Check what git-annex knows about
the file:

.. code-block:: console

   $ git annex whereis path/to/file

- If no ``http(s)://`` URLs are listed and no listed remote is reachable over
  HTTP(S), ``datalad-fuse`` cannot read the content; use ``datalad get``,
  which can also use SSH remotes and other special remotes.
- If URLs are listed, try one of them with e.g. ``curl -I <URL>``: the server
  may be down, or require authentication, which ``datalad-fuse`` does not
  support.
- If ``backends=`` in the message lists only backends that do not handle this
  kind of file — ``backends=remfile`` for anything that is not HDF5-structured
  — no backend was even tried.  Add ``fsspec`` to the list (see
  :ref:`concepts-backends`).

  Up to 0.6.0 this message read ``Could not find a usable URL for <path>
  within <dataset>``.

Errors mentioning "identifier is not of specified type"
-------------------------------------------------------

For example ``RuntimeError: Unable to synchronously get dataspace (identifier
is not of specified type)`` or ``OSError: Can't synchronously read data
(identifier is not of specified type)``, from h5py when reading NWB/HDF5 data.
The data are read lazily, after the file was already closed.  Read the data
while the file is open, inside the ``with`` blocks (see :doc:`python`).

``AttributeError: 'NoneType' object has no attribute 'get_commit_date'``
------------------------------------------------------------------------

The path given to `~datalad_fuse.adapter.DatasetAdapter` is not a dataset (or
git repository).  Check the path, and the current directory if the path is
relative.

``FileNotFoundError`` with ``DatasetAdapter.open()`` or ``datalad fsspec-head``
-------------------------------------------------------------------------------

Paths of files are relative to the top directory of the dataset, not to the
current directory: use ``sub-01/file.nwb`` rather than
``dataset/sub-01/file.nwb`` or ``file.nwb``.

Reading a file in the mount fails with an odd error
---------------------------------------------------

When the content of a file cannot be fetched, programs reading it from the
mount report a generic error such as "Input/output error", or even
"Numerical result out of range".  Run ``datalad fsspec-head`` on the same file
to see the actual problem, or mount with ``datalad -l debug fusefs ...`` to
see the URLs that are tried.

"fusefs does not work properly without --foreground"
----------------------------------------------------

Add ``--foreground`` (``-f``), which is currently required; see :doc:`cli`.

"Unable to find libfuse" or "fuse: device not found"
----------------------------------------------------

FUSE is not installed, or not available.  Install it (see
:doc:`installation`).  In a container, the container needs access to the
FUSE device, e.g. with Docker::

   docker run --device /dev/fuse --cap-add SYS_ADMIN ...

"fuse: mountpoint is not empty"
-------------------------------

Mount on an empty directory.

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

"option allow_other only allowed if 'user_allow_other' is set in /etc/fuse.conf"
--------------------------------------------------------------------------------

``--allow-other`` requires the system administrator to add the line
``user_allow_other`` to ``/etc/fuse.conf``.

"Invalid argument" for every file in the mount
----------------------------------------------

If the path of the dataset given to ``datalad fusefs`` contains a symbolic
link, files whose content is not present cannot be read.  Give the real path
of the dataset instead, e.g. ``datalad fusefs -d "$(realpath path/to/dataset)"
...``.

A subdataset's directory is empty
---------------------------------

The subdataset is not installed.  Install it (without content) with ``datalad
get -n path/to/subdataset``, and remount if you are using a FUSE mount.

``ValueError`` "Path not under root dataset" or "is not in the subpath of"
--------------------------------------------------------------------------

A path passed to `~datalad_fuse.adapter.RemoteFilesystemAdapter` was relative.  Use an
absolute ``root`` and absolute paths (see :doc:`python`).

Changes to the dataset are not reflected
----------------------------------------

The state of each file (annexed or not, content present or not) is
remembered by an adapter or a mount once determined.
After ``datalad get``, ``datalad drop``, ``git checkout`` etc. in the
dataset, create a new adapter or remount.

Warning "Backend 'remfile' requested but not available"
-------------------------------------------------------

A backend named in ``--backends`` or in ``datalad.fusefs.backends`` is not
installed, so it is skipped and the remaining ones are used.  Install it (see
:ref:`installation-backends`) or drop it from the list.  When the default
``remfile,fsspec`` is in effect and remfile is missing, nothing is said: that
is the normal case, and fsspec handles everything.

``ValueError`` "No usable backends from spec ..."
--------------------------------------------------

None of the requested backends could be created: either every name is an
uninstalled backend, or the list is empty.  ``ValueError: Unknown backend:
'...'`` instead means a name that does not exist; the names are ``remfile``
and ``fsspec``.

A file is read by the wrong backend
-----------------------------------

Which backend takes a file is decided from the extension of its annex key,
falling back to the extension of its path (see :ref:`concepts-backends`).  Run
``datalad -l debug fsspec-head ...`` to see the decision; the ``cannot handle
(suffix=..., mode=...)`` lines show what each backend was offered.  Force a
single backend with ``--backends fsspec`` or ``--backends remfile``, for
example to check whether a problem is specific to one of them.

Reading is slow
---------------

- The first access to a remote file takes a moment, to query git-annex and to
  connect to the server.
- Data are fetched in blocks of 5 MiB (fsspec) or chunks of 100 KiB
  (remfile), so reading many small, scattered pieces of a file is slow.  For
  NWB/HDF5 files, installing remfile (see :ref:`installation-backends`)
  usually helps.
- Unreachable URLs delay the fall-through to the next one.  ``git annex
  whereis`` shows which URLs are recorded; ``datalad -l debug`` shows which
  one is slow.
- Reading entire files, or decompressing them, fetches everything, and is
  faster with ``datalad get``.
- Use ``--caching ondisk`` (``caching=True`` in Python) if the same files are
  read repeatedly.

Warnings "Retrying request to ..."
----------------------------------

A server answered with an error (HTTP 5xx), and the request is retried
automatically, up to four times.  Occasional retries are harmless.

Warning "Destroying fsspecs and collection of N fhs"
----------------------------------------------------

Printed by ``datalad fusefs`` when it unmounts; it is harmless.


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

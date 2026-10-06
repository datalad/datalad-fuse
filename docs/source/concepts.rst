How it works
************

This page explains what ``datalad-fuse`` does when a file is opened, where it
looks for content, which *backend* fetches it, and what that means for
performance and caching.  The details apply equally to the FUSE mount, the
Python adapters and ``datalad fsspec-head``, which all share the same
machinery.


Annexed files and their content
===============================

In a DataLad dataset or git-annex repository, small files are typically
committed to git directly, while large files are *annexed*: git only tracks a
symlink whose target encodes a *key*, such as
``SHA256E-s15657857--43b3...a09.nwb``, identifying the file's content by
checksum and size.  The content itself is in the repository's annex
(``.git/annex/objects/``) only if it was added or downloaded there; otherwise
git-annex knows which *remotes* have a copy, and for some of them, URLs to
download it from.

For every file it opens, ``datalad-fuse`` determines one of three states
(`~datalad_fuse.adapter.FileState`):

``NOT_ANNEXED``
   The file is in git (or untracked); it is read from disk.
``HAS_CONTENT``
   The file is annexed and its content is present locally; it is read from
   disk.
``NO_CONTENT``
   The file is annexed but its content is not present locally; it is read
   from a remote URL, as described below.

Only the last case involves the network, so a dataset can mix locally present
files and remote ones freely.


Where the content comes from
============================

For an annexed file without local content, ``datalad-fuse`` builds a list of
candidate URLs from what git-annex knows about the file's key, and tries them
in order until one can be opened:

1. **URLs recorded in git-annex** for the key, as listed by ``git annex
   whereis``: for example URLs registered with ``git annex addurl``, ``datalad
   addurls`` or ``datalad download-url`` (the ``web`` special remote), or
   provided by other special remotes.  Only ``http://`` and ``https://`` URLs
   are used.

2. **http(s) git remotes** that git-annex lists as having the key.  The
   content of an annexed file is then expected at
   ``<remote URL>/.git/annex/objects/<hash directories>/<key>/<key>`` or, for
   a bare repository, ``<remote URL>/annex/objects/...``.  This works for
   repositories served over plain HTTP(S), e.g. a dataset cloned with
   ``datalad clone https://example.com/dataset/.git`` from a web server that
   serves its ``.git`` directory.

   Remotes on `Forgejo-aneksajo <https://codeberg.org/forgejo-aneksajo/forgejo-aneksajo>`_
   instances (such as `hub.datalad.org <https://hub.datalad.org>`_) are
   detected automatically, and their dedicated ``annex/objects`` HTTP endpoint
   is used.

   .. versionadded:: 0.6.0
      Support for Forgejo-aneksajo.

3. **S3 special remotes in export mode** (configured with ``exporttree=yes``
   and a ``publicurl``), as a last resort.  The file's URL is built from its
   path in the dataset, and since such a bucket may hold several versions of
   a file, the version whose size matches the size recorded in the annex key
   is used.  This helps with datasets, such as some older OpenNeuro datasets,
   in which git-annex lacks URLs to specific object versions.  If the object
   versions cannot be listed, the file's current version is used, with a
   warning that it may not be the right one.

   .. note::
      This fallback is newer than the 0.6.0 release.

If no backend can open any of the candidates, opening the file fails with
``Could not open <path> within <dataset> (backends=<backends>)``.

To see which URLs are tried, and by which backend, enable debug logging (see
:ref:`troubleshooting-logging`).

Requests answered with a server error (HTTP 5xx) are retried up to four
times, waiting up to 36 seconds in between, and each retry logs a warning
"Retrying request to ...".  A server that keeps failing can thus delay
opening a file by a couple of minutes before the next URL is tried.
Connection errors are not retried: the next candidate URL is tried right
away.


Access is anonymous
-------------------

URLs are accessed without credentials, unless credentials are part of the URL
itself (e.g. ``https://user:token@host/...``, as can be the case for git
remotes).  DataLad's credential store, ``~/.netrc`` or AWS credentials are not
consulted, so content that requires authentication (for example embargoed
DANDI data or private repositories) cannot be read this way; use ``datalad
get`` for it.

HTTP proxies configured with the ``http_proxy``/``https_proxy`` environment
variables are not used either, so where the internet can only be reached
through a proxy, remote content cannot be read.

Content is not verified
-----------------------

``datalad get`` and ``git annex get`` verify downloaded content against its
key's checksum.  ``datalad-fuse`` reads parts of files and does not verify
them, so it relies on the URLs serving the right content.


.. _concepts-backends:

Backends
========

The candidate URLs above are opened by a *backend*.  Two are available, and
they are tried in order; the first one that both handles the file and manages
to open one of its URLs wins:

.. list-table::
   :header-rows: 1
   :stub-columns: 1
   :widths: 14 43 43

   * -
     - ``remfile``
     - ``fsspec``
   * - Handles
     - HDF5-structured files, by extension: ``.nwb``, ``.h5``, ``.hdf5``,
       ``.hdf``, ``.he5``, ``.nc``, ``.nc4`` (binary reads only)
     - Every file
   * - Fetches
     - 100 KiB chunks, several per request, reading further ahead as long as
       reads stay sequential
     - Blocks of 5 MiB
   * - Installed
     - Optional, with the ``remfile`` extra (see :ref:`installation-backends`)
     - Always, as a dependency of ``datalad-fuse``

The default is ``remfile,fsspec``: NWB/HDF5 files go to `remfile
<https://github.com/flatironinstitute/remfile>`_, which is written for the
many small, scattered reads that HDF5 libraries make, and everything else
goes to fsspec.  If remfile is not installed, it is skipped silently and
fsspec handles everything, as before.

Reading a 73 MB NWB file of a DANDI dandiset from end to end through a FUSE
mount took 4.6 s with remfile and 22.6 s with fsspec; for reads of a few
arrays the two are comparable.  The gain depends on the access pattern, so
measure your own if it matters.

Choosing the backends
---------------------

Give a comma-separated list, in priority order, by any of:

- the ``--backends`` option of ``datalad fusefs`` and ``datalad fsspec-head``;
- the ``datalad.fusefs.backends`` configuration option, for example
  ``git config datalad.fusefs.backends fsspec`` in a dataset, or
  ``datalad -c datalad.fusefs.backends=fsspec ...`` for a single call;
- the ``backends`` argument of the Python adapters (see :doc:`python`).

So ``--backends fsspec`` reads everything with fsspec, and ``--backends
remfile`` reads *only* HDF5-structured files — other files then have no
backend that handles them, and opening them fails.  A name that is requested
explicitly but not installed is skipped with a warning; an unknown name is an
error.

Which file goes to which backend is decided from the extension of the file's
annex key (``SHA256E``, ``MD5E`` and other ``*E`` backends keep it), falling
back to the extension of the file's path for keys that carry none, such as
the ``URL``/``VURL`` keys that ``git annex addurl --fast`` produces.

.. note::
   Backends are newer than the 0.6.0 release.  Up to 0.6.0, all files were
   read with fsspec, which remains the behaviour when remfile is not
   installed.


Reading only what is needed
===========================

Remote files are read with HTTP range requests: by fsspec's `HTTPFileSystem
<https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.http.HTTPFileSystem>`_
in blocks of 5 MiB, or by remfile in chunks of 100 KiB, several of them per
request.  Even reading a few bytes transfers a whole block or chunk, while
reading the metadata and a few arrays of a multi-gigabyte NWB/HDF5 file
transfers only a small fraction of it.

This works best for file formats designed for partial access, such as HDF5
and NWB, and with tools that only read the parts they need.  Reading a whole
file, e.g. to decompress a ``.nii.gz`` file or compute a checksum, transfers
all of it, one request after the other; if you need entire files, ``datalad
get`` is usually faster.

The first access to a remote file takes a little time, as git-annex has to be
queried and a connection established; subsequent reads of the same open file
are faster.


.. _caching:

Caching
=======

By default, fetched data are kept in memory, and only while a file is open.
With caching enabled (``caching=True`` for the Python adapters, ``--caching
ondisk`` for ``datalad fusefs`` and ``datalad fsspec-head``), the backends
store what they fetch on disk and reuse it when the same file is read again,
also in later sessions.  fsspec uses its `CachingFileSystem
<https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.cached.CachingFileSystem>`_,
which keeps blocks for up to a week and fetches them again after that;
remfile keeps its chunks without an expiry.

- The caches of a dataset are located inside it, under
  ``.git/datalad/cache/``, one directory per backend (``fsspec/`` and
  ``remfile/``), so each (sub)dataset has its own.
- Files are cached *sparsely*: only the blocks or chunks that were read are
  stored.
- The cache is separate from the git-annex object store: cached files do not
  count as present content for ``git annex`` or ``datalad``.
- The cache does not shrink by itself.  Remove it with
  ``datalad fsspec-cache-clear`` (add ``-r`` to include subdatasets), or have
  ``datalad fusefs`` remove it on exit by setting the
  ``datalad.fusefs.cache-clear`` configuration option (see
  :ref:`cli-cache-clear`).


Datasets with subdatasets
=========================

The FUSE mount, `~datalad_fuse.adapter.RemoteFilesystemAdapter` and ``datalad
fsspec-head`` work across dataset boundaries: for each file, they determine
the (sub)dataset it belongs to and query that dataset's git-annex.

Subdatasets are not installed automatically, though.  An uninstalled
subdataset appears as an empty directory.  Install the subdatasets you need
first, without getting any content, for example:

.. code-block:: console

   $ datalad get -n -d . path/to/subdataset


.. _concepts-read-only:

Read-only access
================

``datalad-fuse`` is meant for reading.  It does not add anything to the
annex, and files cannot be created, modified, renamed or deleted through the
FUSE mount.  Use ``datalad get`` to keep content locally, and DataLad
commands in the dataset itself (not in the mount) to make changes.

.. note::
   A few operations are currently passed through the mount to the dataset's
   working tree: creating and removing directories, creating special files
   (such as FIFOs), and changing permissions, ownership and timestamps.  Avoid
   them in the mount.

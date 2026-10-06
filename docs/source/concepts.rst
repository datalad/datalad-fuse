How it works
************

This page explains what ``datalad-fuse`` does when a file is opened, where it
looks for content, and what that means for performance and caching.  The
details apply equally to the FUSE mount, the Python adapters and ``datalad
fsspec-head``, which all share the same machinery.


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
(`~datalad_fuse.fsspec.FileState`):

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

If none of the candidates can be opened, opening the file fails with
``Could not find a usable URL for <path> within <dataset>``.

To see which URLs are tried, enable debug logging (see
:ref:`troubleshooting-logging`).

Requests that fail with connection errors or server errors (HTTP 5xx) are
retried a few times with increasing delays (each retry logs a warning
"Retrying request to ...").


Access is anonymous
-------------------

URLs are accessed without credentials, unless credentials are part of the URL
itself (e.g. ``https://user:token@host/...``, as can be the case for git
remotes).  DataLad's credential store, ``~/.netrc`` or AWS credentials are not
consulted, so content that requires authentication (for example embargoed
DANDI data or private repositories) cannot be read this way; use ``datalad
get`` for it.

Content is not verified
-----------------------

``datalad get`` and ``git annex get`` verify downloaded content against its
key's checksum.  ``datalad-fuse`` reads parts of files and does not verify
them, so it relies on the URLs serving the right content.


Reading only what is needed
===========================

Remote files are read with HTTP range requests, through fsspec's
`HTTPFileSystem
<https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.http.HTTPFileSystem>`_.
Data are fetched in blocks of 5 MiB, so even reading a few bytes transfers up
to 5 MiB, while reading the metadata and a few arrays of a multi-gigabyte
NWB/HDF5 file transfers only a small fraction of it.

This works best for file formats designed for partial access, such as HDF5
and NWB, and with tools that only read the parts they need.  Reading a whole file, e.g. to decompress a ``.nii.gz`` file or compute
a checksum, transfers all of it, and over many small requests; if you need
entire files, ``datalad get`` is usually faster.

The first access to a remote file takes a little time, as git-annex has to be
queried and a connection established; subsequent reads of the same open file
are faster.


.. _caching:

Caching
=======

By default, fetched data are kept in memory, and only while a file is open.
With caching enabled (``caching=True`` for the Python adapters, ``--caching
ondisk`` for ``datalad fusefs`` and ``datalad fsspec-head``), fsspec's
`CachingFileSystem
<https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.cached.CachingFileSystem>`_
stores the fetched blocks on disk and reuses them when the same file is read
again, including in later sessions.

- The cache of a dataset is located at ``.git/datalad/cache/fsspec/`` inside
  that dataset, so each (sub)dataset has its own.
- Files are cached *sparsely*: only the blocks that were read are stored.
- The cache is separate from the git-annex object store: cached files do not
  count as present content for ``git annex`` or ``datalad``.
- The cache is never cleaned up automatically while in use.  Remove it with
  ``datalad fsspec-cache-clear`` (add ``-r`` to include subdatasets), or have
  ``datalad fusefs`` remove it on exit by setting the
  ``datalad.fusefs.cache-clear`` configuration option (see
  :ref:`cli-cache-clear`).


Datasets with subdatasets
=========================

The FUSE mount, `~datalad_fuse.fsspec.FsspecAdapter` and ``datalad
fsspec-head`` work across dataset boundaries: for each file, they determine
the (sub)dataset it belongs to and query that dataset's git-annex.

Subdatasets are not installed automatically, though.  An uninstalled
subdataset appears as an empty directory.  Install the subdatasets you need
first, without getting any content, for example:

.. code-block:: console

   $ datalad get -n -d . path/to/subdataset


Read-only access
================

``datalad-fuse`` is meant for reading.  It does not add anything to the
annex, and files cannot be created, modified, renamed or deleted through the
FUSE mount.  Use ``datalad get`` to keep content locally, and DataLad
commands in the dataset itself (not in the mount) to make changes.

.. note::
   A few operations are currently passed through the mount to the dataset's
   working tree: creating and removing directories, and changing permissions,
   ownership and timestamps.  Avoid them in the mount.

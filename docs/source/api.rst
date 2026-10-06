Python API reference
********************

See :doc:`python` for an introduction with examples.

DataLad commands
================

The commands added to DataLad by ``datalad-fuse``, available from
``datalad.api`` and as methods of `datalad.api.Dataset`.

.. currentmodule:: datalad.api
.. autosummary::
   :toctree: generated

   fusefs
   fsspec_head
   fsspec_cache_clear


Opening files: ``datalad_fuse.adapter``
=======================================

.. currentmodule:: datalad_fuse.adapter

.. autoclass:: DatasetAdapter
   :members: open, get_file_state, get_urls, clear, close

.. autoclass:: RemoteFilesystemAdapter
   :members: open, get_file_state, is_under_annex, get_commit_datetime,
             resolve_dataset, get_dataset_path

.. autoclass:: FileState
   :members:
   :undoc-members:

.. autofunction:: resolve_backends

.. autofunction:: create_backends

.. note::
   Up to 0.6.0 these lived in ``datalad_fuse.fsspec``, and
   `RemoteFilesystemAdapter` was called ``FsspecAdapter``.  Importing
   ``FsspecAdapter``, ``DatasetAdapter``, ``FileState`` or ``is_http_url``
   from ``datalad_fuse.fsspec`` still works, with a
   :exc:`DeprecationWarning`.


Backends: ``datalad_fuse.backends``
===================================

Backends do the actual reading of remote files; see :ref:`concepts-backends`.

.. currentmodule:: datalad_fuse.backends

.. autoclass:: Backend
   :members: can_handle, open_url, clear

.. autodata:: DEFAULT_BACKENDS

.. currentmodule:: datalad_fuse.fsspec

.. autoclass:: FsspecBackend
   :members: can_handle, open_url, clear

.. currentmodule:: datalad_fuse.remfile

.. autoclass:: RemfileBackend
   :members: can_handle, open_url, clear

.. autoclass:: RemfileWrapper
   :members: read, seek, tell, info, close


git-annex helpers: ``datalad_fuse.utils``
=========================================

.. currentmodule:: datalad_fuse.utils

.. autoclass:: AnnexKey
   :members: parse, parse_filename

.. autoclass:: AnnexDir

.. autofunction:: is_annex_dir_or_key


FUSE file system: ``datalad_fuse.fuse_``
========================================

.. currentmodule:: datalad_fuse.fuse_

.. autoclass:: DataLadFUSE
   :no-members:

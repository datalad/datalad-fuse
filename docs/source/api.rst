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


Opening files: ``datalad_fuse.fsspec``
======================================

.. currentmodule:: datalad_fuse.fsspec

.. autoclass:: DatasetAdapter
   :members: open, get_file_state, get_urls, clear, close

.. autoclass:: FsspecAdapter
   :members: open, get_file_state, is_under_annex, get_commit_datetime,
             resolve_dataset, get_dataset_path

.. autoclass:: FileState
   :members:
   :undoc-members:


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

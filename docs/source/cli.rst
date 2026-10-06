Command-line usage
******************

``datalad-fuse`` adds three commands to DataLad:

.. list-table::
   :widths: 30 70

   * - ``datalad fusefs``
     - Mount a dataset with FUSE, so that any program can read its files.
   * - ``datalad fsspec-head``
     - Print the first lines or bytes of a file.
   * - ``datalad fsspec-cache-clear``
     - Remove the on-disk cache of fetched data.

This page shows how to use them; :doc:`cmdline` lists all of their options.
Each command is also available from Python, see :ref:`python-commands`.


Mounting a dataset: ``datalad fusefs``
======================================

.. code-block:: console

   $ mkdir mnt
   $ datalad fusefs -d path/to/dataset --foreground mnt

mounts the dataset at ``path/to/dataset`` on the existing, empty directory
``mnt`` (the *mount point*).  Without ``-d``, the dataset containing the
current directory is mounted.  Create the mount point outside of the dataset:
a mount point inside it would show up in the mount itself, and as an
untracked directory in ``datalad status``.

The ``--foreground`` (``-f``) option is currently required: the command keeps
running for as long as the dataset is mounted.  Run it in a terminal of its
own, or in a ``tmux``/``screen`` session.  When running it in the background
of a shell (appending ``&``), wait until the mount is ready before using it,
e.g. with ``until mountpoint -q mnt; do sleep 0.1; done``.

To unmount, press :kbd:`Ctrl-C` in the terminal where ``datalad fusefs`` runs
(for a background job, bring it to the foreground with ``fg`` first), or run:

.. code-block:: console

   $ fusermount -u mnt

What you see in the mount
-------------------------

- The same directory tree as in the dataset's working tree.
- Annexed files appear as regular files, whether or not their content is
  present.  Their size comes from the annex key, and their modification time
  is the date of the dataset's last commit.  Reading a file whose content is
  not present fetches the needed parts from a remote URL (see
  :doc:`concepts`).
- The ``.git`` directory is hidden (see ``--mode-transparent`` below).
- Installed subdatasets are included; uninstalled ones are empty directories.
- Files cannot be written, created or deleted.

Options
-------

``--caching ondisk``
   Keep fetched data in an on-disk cache for reuse, also by later mounts
   (see :ref:`caching`).  The default, ``none``, only buffers data in memory
   while a file is open.

``--allow-other``
   Let other users access the mount; by default, only the user who mounted
   it can.  This requires the line ``user_allow_other`` in
   ``/etc/fuse.conf``.

``--mode-transparent``
   Show the ``.git`` directories.  Annexed files then appear as the symlinks
   they are in the dataset, and the targets of these symlinks under
   ``.git/annex/objects/`` can be read, with their content fetched as
   needed.  Files under ``.git`` can also be written to.

.. _cli-cache-clear:

Clearing the cache on exit
--------------------------

The configuration option ``datalad.fusefs.cache-clear`` makes ``datalad
fusefs`` remove on-disk caches when it exits:

``visited``
   Clear the caches of the (sub)datasets that were accessed in the mount.
``recursive``
   Clear the caches of the mounted dataset and all its installed
   subdatasets.

Set it like any DataLad or git configuration option, permanently (e.g.
``git config --global datalad.fusefs.cache-clear visited``) or for a single
call:

.. code-block:: console

   $ datalad -c datalad.fusefs.cache-clear=visited fusefs -d ds --foreground --caching ondisk mnt


Peeking into a file: ``datalad fsspec-head``
============================================

Prints the first lines (10 by default) or bytes of a file to standard
output, fetching only what is needed.  It is a quick way to check that the
content of a file can be reached, or to look at the header of a file:

.. code-block:: console

   $ datalad fsspec-head -d path/to/dataset -n 5 data/participants.tsv
   $ datalad fsspec-head -d path/to/dataset -c 8 data/recording.nwb | od -c

.. note::
   Relative paths are interpreted relative to the top directory of the
   dataset, not to the current directory.

The output is the raw content of the file, without any result rendering, so
it can be piped into other tools.  ``--caching ondisk`` stores the fetched
data in the dataset's cache.


Clearing the cache: ``datalad fsspec-cache-clear``
==================================================

Removes the on-disk cache of a dataset (``.git/datalad/cache/fsspec/``):

.. code-block:: console

   $ datalad fsspec-cache-clear -d path/to/dataset

Add ``-r`` (``--recursive``) to also clear the caches of all installed
subdatasets.

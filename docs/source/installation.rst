Installation
************

Requirements
============

- **Python** 3.9 or newer.
- **git-annex**, as for DataLad itself.  Install it with your system's package
  manager (e.g. ``sudo apt-get install git-annex`` on Debian/Ubuntu, ``brew
  install git-annex`` on macOS), from conda-forge (``conda install -c
  conda-forge git-annex``), or into your Python environment with ``pip install
  git-annex``.  The `DataLad Handbook
  <https://handbook.datalad.org/en/latest/intro/installation.html>`_ covers
  more options.
- **FUSE**, only for mounting datasets with ``datalad fusefs``.  The
  `mfusepy <https://github.com/mxmlnkn/mfusepy>`_ bindings used by
  ``datalad-fuse`` work with the FUSE 3 or the FUSE 2 library, together with
  its ``fusermount3`` or ``fusermount`` helper.  On Debian and Ubuntu::

        sudo apt-get install fuse3

  (``sudo apt-get install fuse`` on releases without the ``fuse3`` package).
  If both libraries are installed, FUSE 2 is used; set the environment
  variable ``FUSE_LIBRARY_NAME=fuse3`` to use FUSE 3 instead.

  FUSE mounts are developed and tested on Linux.  Opening files from Python
  (see :doc:`python`) and ``datalad fsspec-head`` do not need FUSE at all.


Installing datalad-fuse
=======================

Install the latest release from `PyPI <https://pypi.org/project/datalad-fuse>`_
with pip; DataLad, fsspec and the other Python dependencies are installed
along with it:

.. code-block:: console

   $ python3 -m pip install datalad-fuse

This documentation describes the development version, which may include
features that are not released yet.  To install it:

.. code-block:: console

   $ python3 -m pip install git+https://github.com/datalad/datalad-fuse.git


Checking the installation
=========================

``datalad-fuse`` is a DataLad extension: it adds commands to DataLad.  These
commands should print their help, and the version of git-annex and of
``datalad-fuse``:

.. code-block:: console

   $ datalad fusefs --help
   $ git annex version
   $ python3 -c "import datalad_fuse; print(datalad_fuse.__version__)"

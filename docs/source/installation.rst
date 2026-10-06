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
  `fusepy <https://github.com/fusepy/fusepy>`_ bindings used by
  ``datalad-fuse`` need the FUSE 2 library (``libfuse.so.2``), plus the
  ``fusermount`` helper, which FUSE 3 also provides.  Recent Debian and
  Ubuntu releases come with FUSE 3 only; add the FUSE 2 library to it, keeping
  FUSE 3 installed:

  - Debian 13 (trixie), Ubuntu 24.04 and newer::

        sudo apt-get install fuse3 libfuse2t64

  - Debian 12 (bookworm), Ubuntu 22.04::

        sudo apt-get install fuse3 libfuse2

  - older releases::

        sudo apt-get install fuse

  On these releases, installing the ``fuse`` package instead would remove
  ``fuse3``, and with it possibly other packages that depend on it.

  FUSE mounts are developed and tested on Linux.  Opening files from Python
  (see :doc:`python`) and ``datalad fsspec-head`` do not need FUSE at all.


Installing datalad-fuse
=======================

Install the latest release from `PyPI <https://pypi.org/project/datalad-fuse>`_
with pip; DataLad, fsspec and the other Python dependencies are installed
along with it:

.. code-block:: console

   $ python3 -m pip install datalad-fuse

To install the development version, which may include features that are not
released yet:

.. code-block:: console

   $ python3 -m pip install git+https://github.com/datalad/datalad-fuse.git


.. _installation-backends:

Optional backends
=================

Remote files are read by a *backend* (see :ref:`concepts-backends`).  fsspec,
which handles every kind of file, is always installed.  `remfile
<https://github.com/flatironinstitute/remfile>`_, which is faster for
NWB/HDF5 files, is optional:

.. code-block:: console

   $ python3 -m pip install "datalad-fuse[remfile]"

The ``full`` extra installs every optional backend, currently the same one:

.. code-block:: console

   $ python3 -m pip install "datalad-fuse[full]"

Installing remfile is enough to start using it: NWB/HDF5 files are read with
it by default from then on.  Without it, those files are read with fsspec, as
before.

.. note::
   The optional backends are newer than the 0.6.0 release.


Checking the installation
=========================

``datalad-fuse`` is a DataLad extension: it adds commands to DataLad.  Check
that DataLad finds them and that git-annex is available:

.. code-block:: console

   $ datalad fusefs --help
   Usage: datalad fusefs [-h] [-d DATASET] [-f] [--mode-transparent]
   ...
   $ git annex version
   git-annex version: 10.20260901
   ...

and that the Python package can be imported:

.. code-block:: console

   $ python3 -c "import datalad_fuse; print(datalad_fuse.__version__)"

To check whether the optional remfile backend is available:

.. code-block:: console

   $ python3 -c "import remfile; print(remfile.__file__)"

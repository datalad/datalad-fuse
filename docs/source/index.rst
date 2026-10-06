datalad-fuse
############

*Open files of DataLad datasets and git-annex repositories without
downloading them first.*

When you clone a `DataLad <https://www.datalad.org>`_ dataset or a
`git-annex <https://git-annex.branchable.com>`_ repository, you get all file
names and the full history, but the content of the large ("annexed") files
stays where it was published: for example in the S3 storage of `DANDI
<https://dandiarchive.org>`_ or `OpenNeuro <https://openneuro.org>`_, or on a
web server.  ``datalad get`` downloads such files in full before you can open
them.

``datalad-fuse`` lets you open them right away instead.  It looks up the URLs
that git-annex has recorded for a file and, using
`fsspec <https://filesystem-spec.readthedocs.io>`_, fetches only the parts of
the file that are actually read.

Is it a good fit?
=================

``datalad-fuse`` pays off when you need a *fraction* of large files:

- some arrays or tables out of NWB/HDF5 files, without the raw data;
- the headers of many imaging files;
- small metadata or sidecar files spread over a large dataset;
- a quick look into a dataset that would not fit on your disk.

When you need whole files, for example the voxel data of compressed NIfTI
(``.nii.gz``) images, which must be decompressed from the start, ``datalad
get`` is usually faster, and keeps the files for later.

There are two ways to use it:

.. list-table::
   :header-rows: 1
   :stub-columns: 1
   :widths: 16 42 42

   * -
     - FUSE mount
     - Python
   * - What
     - ``datalad fusefs`` presents a dataset as a read-only directory tree in
       which annexed files can be opened like local files.
     - ``FsspecAdapter`` and ``DatasetAdapter`` return Python file objects
       for files of a dataset.
   * - Works with
     - Any program: ``h5ls``, MATLAB, ``nwbinspector``, shell tools, ...
     - Python libraries that accept file objects: h5py, pynwb, pandas,
       json, ...
   * - Needs
     - Linux with FUSE installed
     - Just Python (nothing to install beyond ``datalad-fuse``)

Use Python if the library you read files with accepts Python file objects
(h5py, PyNWB, pandas, ...); use the FUSE mount for tools that need a file
name: MATLAB, FSL, command-line tools, or Python functions that only accept
paths.


A quick look
============

Get a dataset, here a dandiset from the `DANDI Archive
<https://dandiarchive.org>`_, which is small to clone (about 2 MB) although its
files add up to 1.86 GB:

.. code-block:: console

   $ datalad clone https://github.com/dandisets/000582

(OpenNeuro datasets are cloned the same way, e.g. ``datalad clone
https://github.com/OpenNeuroDatasets/ds000001``.)

Mount it.  ``datalad fusefs`` keeps running until the dataset is unmounted:

.. code-block:: console

   $ mkdir mnt
   $ datalad fusefs -d 000582 --foreground mnt

Then, in another terminal, use any tool on its files, e.g. ``h5ls`` from the
HDF5 tools, and unmount when done:

.. code-block:: console

   $ h5ls mnt/sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb
   acquisition              Group
   analysis                 Group
   ...
   units                    Group
   $ fusermount -u mnt

Or open files directly from Python:

.. code-block:: python

   from contextlib import closing

   import h5py
   import pynwb

   from datalad_fuse.fsspec import DatasetAdapter

   # caching=False: keep fetched data in memory only (see "Caching")
   with closing(DatasetAdapter("000582", caching=False)) as dsa:
       with dsa.open("sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb") as f:
           with h5py.File(f, "r") as h5, pynwb.NWBHDF5IO(file=h5) as io:
               nwbfile = io.read()
               print(nwbfile.units.to_dataframe())

The :doc:`tutorial` walks through both in more detail.


.. toctree::
   :maxdepth: 2
   :caption: Getting started

   installation
   tutorial

.. toctree::
   :maxdepth: 2
   :caption: User guide

   concepts
   cli
   python
   troubleshooting

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api
   cmdline
   changelog

.. toctree::
   :maxdepth: 1
   :caption: Development

   contributing


Indices
=======

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

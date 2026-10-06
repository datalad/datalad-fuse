datalad-fuse
############

*Open files of DataLad datasets and git-annex repositories without
downloading them first.*

When you clone a `DataLad <https://www.datalad.org>`_ dataset or a
`git-annex <https://git-annex.branchable.com>`_ repository, you get all file
names and the full history, but the content of the large ("annexed") files
stays where it was published: on a web server, in an S3 bucket, on a
Forgejo instance, ...  ``datalad get`` downloads such files in full before you
can open them.

``datalad-fuse`` lets you open them right away instead.  It looks up the URLs
that git-annex has recorded for a file and, using
`fsspec <https://filesystem-spec.readthedocs.io>`_, fetches only the parts of
the file that are actually read.  This pays off whenever you need a fraction
of large files: the header of an imaging file, one table out of an NWB/HDF5
file, the metadata of thousands of files, or a dataset that would not fit on
your disk.

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
     - Linux with FUSE
     - Just Python (no FUSE, no administrator rights)


A quick look
============

Get a dataset, here a dandiset from the `DANDI Archive
<https://dandiarchive.org>`_, which is small to clone (about 2 MB) although its
files add up to 1.86 GB:

.. code-block:: console

   $ datalad clone https://github.com/dandisets/000582

Mount it and use any tool on its files:

.. code-block:: console

   $ mkdir mnt
   $ datalad fusefs -d 000582 --foreground mnt &
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

Tutorial: streaming NWB data from DANDI
***************************************

This tutorial opens neurophysiology data from the `DANDI Archive
<https://dandiarchive.org>`_ without downloading entire files, first from
Python and then through a FUSE mount.  It follows the DANDI tutorial
`Streaming and interacting with NWB data from DANDI
<https://docs.dandiarchive.org/example-notebooks/tutorials/bcm_2024/analysis-demo/>`_,
but gets the data through git-annex instead of the DANDI API.

The data are position tracking and spike times recorded from the medial
entorhinal cortex of rats (`Dandiset 000582
<https://dandiarchive.org/dandiset/000582>`_; Sargolini et al., *Science*,
2006), stored in `NWB <https://nwb.org>`_ files.


Setup
=====

Besides ``datalad-fuse`` and git-annex (see :doc:`installation`), install
the packages used to read and plot the data:

.. code-block:: console

   $ python3 -m pip install datalad-fuse h5py pynwb matplotlib

The FUSE part of the tutorial also uses ``h5ls`` from the HDF5 command-line
tools (``sudo apt-get install hdf5-tools`` on Debian/Ubuntu, or ``conda install
-c conda-forge hdf5``); any other program that reads files would do as well.

Run all commands and Python code from the same directory: the one in which
you clone the dandiset (so that it contains ``000582/``).


Get the dandiset
================

Public dandisets are mirrored as git-annex repositories on GitHub, under
https://github.com/dandisets.  Each asset is an annexed file, for which
git-annex knows the URLs of its content on the archive's S3 bucket and on
the DANDI API.  Clone the dandiset with DataLad (or plain ``git clone``):

.. code-block:: console

   $ datalad clone https://github.com/dandisets/000582
   [INFO] Attempting a clone into /home/me/000582
   ...
   [INFO] access to 2 dataset siblings dandi-dandisets-dropbox, dandiapi not auto-enabled, enable with:
   | 		datalad siblings -d "/home/me/000582" enable -s SIBLING
   install(ok): /home/me/000582 (dataset)
   $ du -sh 000582
   2.2M	000582

You can ignore the ``[INFO]`` messages, including the suggestion to enable
siblings: ``datalad-fuse`` does not need them.

The clone holds all file names but no annexed content, which would be
1.86 GB.  Annexed files are symlinks that point to content which is not
there:

.. code-block:: console

   $ ls -l 000582/sub-10073/
   lrwxrwxrwx 1 me me 203 Oct  6 19:35 sub-10073_ses-17010302_behavior+ecephys.nwb -> ../.git/annex/objects/vq/Z5/SHA256E-s15657857--43b3....nwb/SHA256E-s15657857--43b3....nwb

``git annex whereis`` shows where the content of a file can be found:

.. code-block:: console

   $ git -C 000582 annex whereis sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb
   whereis sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb (1 copy)
     	00000000-0000-0000-0000-000000000001 -- web
   ...
     web: https://api.dandiarchive.org/api/assets/2b9e441b-56bc-4be2-893e-0e02d22d239d/download/
     web: https://dandiarchive.s3.amazonaws.com/blobs/26a/22c/26a22c31-09bc-43a4-9187-edc7394ed12c?versionId=__7hm7itizkF8RCsvO.Fidzi7Lqd1OMu
   ok

The ``web:`` lines are the URLs that ``datalad-fuse`` will read the content
from, trying them in this order.

A quick check that the content can be reached: ``datalad fsspec-head``
prints the first bytes of the file, here the signature of an HDF5 file:

.. code-block:: console

   $ datalad fsspec-head -d 000582 -c 8 sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb | od -c
   0000000 211   H   D   F  \r  \n 032  \n
   0000010


Read the data from Python
=========================

`DatasetAdapter <datalad_fuse.fsspec.DatasetAdapter>` gives access to the
files of one dataset, addressed by paths relative to the dataset's top
directory.  Its ``open()`` method returns a file object that h5py, and hence
PyNWB, can read from:

.. code-block:: python

   from contextlib import closing

   import h5py
   import matplotlib.pyplot as plt
   import numpy as np
   import pynwb

   from datalad_fuse.fsspec import DatasetAdapter

   nwb_path = "sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb"

   # caching=False: keep fetched data in memory only (see "Keep fetched data
   # in a cache" below)
   with closing(DatasetAdapter("000582", caching=False)) as dsa:
       with dsa.open(nwb_path) as f, h5py.File(f, "r") as h5:
           with pynwb.NWBHDF5IO(file=h5) as io:
               nwbfile = io.read()
               print(nwbfile.subject)

               # Position of the animal, from the first LED on its head
               position = nwbfile.processing["behavior"]["Position"]["SpatialSeriesLED1"]
               ts = position.timestamps[:]
               x = position.data[:, 0]

               # Sorted units, with the spike times of each unit
               units_df = nwbfile.units.to_dataframe()

   plt.plot(ts, x)
   plt.xlabel("Time (seconds)")
   plt.ylabel("X coordinate of the subject")
   plt.show()

   fig, ax = plt.subplots()
   for i, spike_times in enumerate(units_df["spike_times"]):
       ax.plot(spike_times, np.ones_like(spike_times) + i, "|", markersize=20)
   ax.set(xlabel="Time (s)", ylabel="Unit number")
   plt.show()

Some things to note:

- PyNWB and h5py read data lazily: ``position.data`` is not loaded until it is
  sliced (``position.data[:, 0]``), and only the parts of the file needed for
  that slice are fetched.  Read everything you need *before* the ``with``
  blocks close the file; ``units_df``, ``ts`` and ``x`` above are in-memory
  copies that remain usable afterwards.  Reading from ``position.data`` after
  the file was closed fails with an error mentioning "identifier is not of
  specified type".
- ``closing()`` makes sure that the ``git annex`` processes started by the
  adapter are stopped when you are done.
- No DANDI-specific code was needed: the file was found by its path in the
  dataset, and its URL came from git-annex.  The same code works for any
  dataset whose annexed content is reachable over HTTP(S).

When exploring data interactively, e.g. in Jupyter, ``with`` blocks are
impractical.  Open everything step by step instead, and close it in reverse
order when you are done:

.. code-block:: python

   dsa = DatasetAdapter("000582", caching=True)
   f = dsa.open(nwb_path)
   h5 = h5py.File(f, "r")
   io = pynwb.NWBHDF5IO(file=h5)
   nwbfile = io.read()

   # ... explore nwbfile in further cells ...

   io.close()
   h5.close()
   f.close()
   dsa.close()

From here on, the analysis part of the DANDI tutorial (e.g. computing tuning
curves with `pynapple <https://pynapple.org>`_) applies to ``nwbfile``
unchanged, as long as it runs while the file is open.


Read the data through a FUSE mount
==================================

A FUSE mount gives the same access to programs that expect file names
rather than Python file objects.  First create an empty directory to mount
the dataset on (the *mount point*), next to the dataset rather than inside
it:

.. code-block:: console

   $ mkdir mnt

``datalad fusefs`` keeps running for as long as the dataset is mounted, so
start it in a terminal of its own:

.. code-block:: console

   $ datalad fusefs -d 000582 --foreground mnt

Continue in a second terminal, in the same directory.  In the mount, annexed
files look like regular files, and their content is fetched when it is read:

.. code-block:: console

   $ ls -l mnt/sub-10073/
   -rw-r--r-- 1 me me 15657857 May 28 09:22 sub-10073_ses-17010302_behavior+ecephys.nwb
   $ h5ls mnt/sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb
   acquisition              Group
   analysis                 Group
   file_create_date         Dataset {1}
   general                  Group
   identifier               Dataset {SCALAR}
   processing               Group
   session_description      Dataset {SCALAR}
   session_start_time       Dataset {SCALAR}
   specifications           Group
   stimulus                 Group
   timestamps_reference_time Dataset {SCALAR}
   units                    Group

Python code can now use plain file names:

.. code-block:: python

   import pynwb

   with pynwb.NWBHDF5IO("mnt/sub-10073/sub-10073_ses-17010302_behavior+ecephys.nwb") as io:
       print(io.read().units.to_dataframe())

When done, unmount the dataset, either by pressing :kbd:`Ctrl-C` in the
terminal running ``datalad fusefs``, or with:

.. code-block:: console

   $ fusermount -u mnt

``datalad fusefs`` then exits, printing:

.. code-block:: text

   [WARNING] Destroying fsspecs and collection of 1 fhs
   fusefs(ok): mnt

The warning is harmless (the number varies).  If unmounting fails with "Device or resource busy",
a program still uses the mount (e.g. a shell whose current directory is in
it); see :doc:`troubleshooting`.


Keep fetched data in a cache
============================

By default, fetched data are kept in memory only while a file is open, so
opening the file again fetches the data again.  With caching enabled, they
are stored in a cache on disk and reused, also across sessions:

.. code-block:: python

   with closing(DatasetAdapter("000582", caching=True)) as dsa:
       ...

or, for a mount:

.. code-block:: console

   $ datalad fusefs -d 000582 --foreground --caching ondisk mnt &

The cache is kept inside the dataset, under ``.git/datalad/cache/fsspec/``,
and holds only the parts of files that were read:

.. code-block:: console

   $ du -sh 000582/.git/datalad/cache/fsspec
   10M	000582/.git/datalad/cache/fsspec

Remove it when it is no longer needed:

.. code-block:: console

   $ datalad fsspec-cache-clear -d 000582
   fsspec-cache-clear(ok): /home/me/000582 (dataset)

See :ref:`caching` for more.


Pin the version of the data
===========================

The dandiset repository records every change of the dandiset, and each
published version of the dandiset is a git tag.  Checking out a tag gives you
exactly the files of that version, and ``datalad-fuse`` then reads the content
of those versions of the files:

.. code-block:: console

   $ git -C 000582 tag
   0.251111.2151
   $ git -C 000582 checkout 0.251111.2151
   Note: switching to '0.251111.2151'.

   You are in 'detached HEAD' state. ...

The "detached HEAD" message is expected: you are looking at a past version
rather than at a branch.  To return to the latest state of the dandiset,
check out its main branch, which is called ``draft`` for dandisets:

.. code-block:: console

   $ git -C 000582 checkout draft

After checking out another version, create a new adapter or remount the
dataset, as they do not notice such changes.  Recording the commit (``git -C
000582 rev-parse HEAD``) along with your results tells exactly which data they
were computed from.


Next steps
==========

- :doc:`concepts` explains where ``datalad-fuse`` looks for content, and what
  that means for performance and caching.
- :doc:`cli` and :doc:`python` describe the command line and Python
  interfaces in detail.
- :doc:`troubleshooting` helps when something does not work.

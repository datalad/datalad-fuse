#!/usr/bin/env python
import os.path
import sys

from setuptools import setup

# This is needed for versioneer to be importable when building with PEP 517.
# See <https://github.com/warner/python-versioneer/issues/193> and links
# therein for more information.
sys.path.append(os.path.dirname(__file__))

from _datalad_buildsupport.setup import BuildManPage  # noqa: E402
import versioneer  # noqa: E402

cmdclass = versioneer.get_cmdclass()
cmdclass.update(build_manpage=BuildManPage)

if __name__ == "__main__":
    setup(
        name="datalad-fuse",
        version=versioneer.get_version(),
        cmdclass=cmdclass,
    )

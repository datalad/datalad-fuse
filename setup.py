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

# Give setuptools a hint to complain if it's too old a version
# 30.3.0 allows us to put most metadata in setup.cfg
# Should match pyproject.toml
SETUP_REQUIRES = ["setuptools >= 30.3.0"]
# This enables setuptools to install wheel on-the-fly
SETUP_REQUIRES += ["wheel"] if "bdist_wheel" in sys.argv else []

if __name__ == "__main__":
    setup(
        name="datalad-fuse",
        version=versioneer.get_version(),
        cmdclass=cmdclass,
        setup_requires=SETUP_REQUIRES,
    )

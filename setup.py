# -*- coding: utf-8 -*-

import os
import re
from typing import List

from setuptools import setup, find_packages


def read(fname: str) -> str:
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def read_version():
    with open("pyperunner/version.py", "r") as f:
        c = f.readline()
    g = re.match('__version__ = "([^"]+)"', c)
    if not g:
        raise RuntimeError("Unable to find version string in __version__.py")
    return g.group(1)


def parse_requirements(filename: str) -> List[str]:
    """ Load requirements from a pip requirements file """
    with open(filename, "r") as fd:
        lines = []
        for line in fd:
            line = line.strip()
            if line and not line.startswith("#"):
                lines.append(line)
    return lines


if __name__ == "__main__":
    readme = read("README.rst")
    changes = read("CHANGELOG.rst")

    requirements = parse_requirements("requirements.txt")

    setup(
        name="pyperunner",
        description="Yet another ETL pipeline runner for python, using multiprocessing and directed acyclic graphs.",
        long_description="\n\n".join([readme, changes]),
        license="GNU General Public License v3",
        url="https://github.com/glichtner/pyperunner/",
        version=read_version(),
        author="Gregor Lichtner",
        maintainer="Gregor Lichtner",
        install_requires=requirements,
        keywords=["pyperunner"],
        packages=find_packages("."),
        zip_safe=False,
        classifiers=[
            # complete list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
            "Development Status :: 4 - Beta",
            "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
            "Operating System :: MacOS",
            "Operating System :: Unix",
            "Operating System :: POSIX",
            "Intended Audience :: Developers",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
        ],
    )

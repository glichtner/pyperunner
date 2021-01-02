# -*- coding: utf-8 -*-

import os
from typing import List

from setuptools import setup, find_packages

from pyperunner.version import __version__


def read(fname: str) -> str:
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def parse_requirements(filename: str) -> List[str]:
    """ Load requirements from a pip requirements file """
    with open(filename, "r") as fd:
        lines = []
        for line in fd:
            line.strip()
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
        version=__version__,
        author="Gregor Lichtner",
        maintainer="Gregor Lichtner",
        install_requires=requirements,
        keywords=["pyperunner"],
        packages=find_packages("."),
        zip_safe=False,
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
        ],
    )

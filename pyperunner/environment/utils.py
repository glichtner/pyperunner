"""
Collection of util functions

The contents of this file have been taken from the sacred project: https://github.com/IDSIA/sacred

The sacred project is released under the MIT license. A copy of the MIT license is supplied in
ThirdPartyNotices.txt in the root directory of this project.

The original file can be found here:
https://github.com/IDSIA/sacred/blob/8fd86e0700461e90cf313c44ab735584a3178def/sacred/utils.py
"""
from collections.abc import Generator


def join_paths(*parts: str) -> str:
    """Join different parts together to a valid dotted path."""
    return ".".join(str(p).strip(".") for p in parts if p)


def iter_prefixes(path: str) -> Generator:
    """
    Iterate through all (non-empty) prefixes of a dotted path.
    Example
    -------
    >>> list(iter_prefixes('foo.bar.baz'))
    ['foo', 'foo.bar', 'foo.bar.baz']
    """
    split_path = path.split(".")
    for i in range(1, len(split_path) + 1):
        yield join_paths(*split_path[:i])

"""
Collection of util functions

The contents of this file have been taken from the sacred project: https://github.com/IDSIA/sacred

The sacred project is released under the MIT license. A copy of the MIT license is supplied in
ThirdPartyNotices.txt in the root directory of this project.

The original file can be found here:
https://github.com/IDSIA/sacred/blob/8fd86e0700461e90cf313c44ab735584a3178def/sacred/utils.py
"""

import importlib
from typing import Tuple


def optional_import(*package_names: str) -> Tuple:
    try:
        packages = [importlib.import_module(pn) for pn in package_names]
        return True, packages[0]
    except ImportError:
        return False, None


has_numpy, np = optional_import("numpy")

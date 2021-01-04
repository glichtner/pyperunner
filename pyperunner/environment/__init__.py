"""
Collect source dependency information

This file uses contents from the sacred project: https://github.com/IDSIA/sacred

The sacred project is released under the MIT license. A copy of the MIT license is supplied in
ThirdPartyNotices.txt in the root directory of this project.
"""
import os
import inspect
import platform
from typing import List, Dict

from .dependencies import gather_sources_and_dependencies, Source, PackageDependency


def _collect_repositories(sources: List[Source]) -> List[Dict[str, str]]:
    return [
        {"url": s.repo, "commit": s.commit, "dirty": s.is_dirty}
        for s in sources
        if s.repo
    ]


def get_environment_info(stack_level: int = 1) -> Dict:
    """Get a dictionary with information about the current environment.

    """
    _caller_globals = inspect.stack()[stack_level][0].f_globals

    mainfile_dir = os.path.dirname(_caller_globals.get("__file__", "."))
    base_dir = os.path.abspath(mainfile_dir)

    mainfile, sources, dependencies = gather_sources_and_dependencies(
        _caller_globals, save_git_info=True
    )

    for dep in dependencies:
        dep.fill_missing_version()

    mainfile = mainfile.to_json(base_dir)["filename"] if mainfile else None

    def name_lower(d: PackageDependency) -> str:
        return d.name.lower()

    return dict(
        base_dir=base_dir,
        sources=[s.to_json(base_dir) for s in sorted(sources)],
        dependencies=[d.to_json() for d in sorted(dependencies, key=name_lower)],
        repositories=_collect_repositories(sources),
        mainfile=mainfile,
    )


def get_host_info() -> Dict[str, str]:
    return {
        "hostname": platform.node(),
        "os": platform.platform(),
        "python": platform.python_version(),
    }

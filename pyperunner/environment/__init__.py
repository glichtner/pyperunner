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

from pyperunner.environment.dependencies import (
    gather_sources_and_dependencies,
    Source,
    PackageDependency,
)


def _collect_repositories(sources: List[Source]) -> List[Dict[str, str]]:
    return [
        {"url": s.repo, "commit": s.commit, "dirty": s.is_dirty}
        for s in sources
        if s.repo
    ]


def get_environment_info(stack_level: int = 1) -> Dict:
    """
    Get a dictionary with information about the current environment.

    The logic of this function was taken from sacred project: https://github.com/IDSIA/sacred

    Args:
        stack_level:

    Returns:
        Dict with information about
        - *base_dir*: path of the main script file
        - *sources*: List of all local source files (not imported via installed packages)
            - *filename*: Filename of the source file relative to base_dir
            - *md5hash*: md5 hash of the source file contents
        - *dependencies*: List of all imported modules/packages with version
        - *repositories*: List of all git repositories involved
            - *commit*: commit hash
            - *url*: git repository remote URL
            - *dirty*: If local working copy is dirty
        - *mainfile*: Filename of the main script file
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
    """
    Gets basic information about the host platform

    Returns: Dict with
        - *hostname*
        - *os*: operating system information
        - *python*: python version

    """
    return {
        "hostname": platform.node(),
        "os": platform.platform(),
        "python": platform.python_version(),
    }

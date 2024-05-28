from __future__ import annotations

import collections
import email
import json
import logging
import pkgutil
import sys
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

from databricks.labs.blueprint.entrypoint import get_logger

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.graph import DependencyProblem
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.getLogger(__name__)


@dataclass
class Compatibility:
    known: bool
    problems: list[DependencyProblem]


UNKNOWN = Compatibility(False, [])
_DEFAULT_ENCODING = sys.getdefaultencoding()


class Whitelist:
    def __init__(self):
        self._module_problems = collections.OrderedDict()
        self._module_distributions = {}
        known = self._get_known()
        for distribution_name, modules in known.items():
            specific_modules_first = sorted(modules.items(), key=lambda x: x[0], reverse=True)
            for module_ref, problems in specific_modules_first:
                self._module_problems[module_ref] = [DependencyProblem(**_) for _ in problems]
                self._module_distributions[module_ref] = distribution_name
        for name in sys.stdlib_module_names:
            self._module_problems[name] = []
            self._module_distributions[name] = "python"

    @staticmethod
    def _get_known():
        module = __name__
        if __name__ == "__main__":  # code path for UCX developers invoking `make known`
            module = "databricks.labs.ucx.source_code.known"
        # load known.json from package data, because we may want to use zipapp packaging
        data = pkgutil.get_data(module, "known.json")
        return json.loads(data)

    def module_compatibility(self, name: str) -> Compatibility:
        if not name:
            return UNKNOWN
        for module, problems in self._module_problems.items():
            if not name.startswith(module):
                continue
            return Compatibility(True, problems)
        return UNKNOWN

    def distribution_compatibility(self, name: str) -> Compatibility:
        if not name:
            return UNKNOWN
        # many packages can belong to a distribution, so we use a loop for matching
        for module, distribution_name in self._module_distributions.items():
            if distribution_name != name:
                continue
            problems = self._module_problems[module]
            return Compatibility(True, problems)
        return UNKNOWN

    @classmethod
    def rebuild(cls, root: Path):
        """rebuild the known.json file by analyzing the source code of installed libraries. Invoked by `make known`."""
        path_lookup = PathLookup.from_sys_path(root)
        known_distributions = cls._get_known()
        for library_root in path_lookup.library_roots:
            for dist_info_folder in library_root.glob("*.dist-info"):
                cls._analyze_dist_info(dist_info_folder, known_distributions, library_root)
        known_json = Path(__file__).parent / "known.json"
        with known_json.open('w') as f:
            json.dump(dict(sorted(known_distributions.items())), f, indent=2)

    @classmethod
    def _analyze_dist_info(cls, dist_info_folder, known_distributions, library_root):
        dist_info = DistInfo(dist_info_folder)
        if dist_info.name in known_distributions:
            logger.debug(f"Skipping distribution: {dist_info.name}")
            return
        logger.info(f"Processing distribution: {dist_info.name}")
        known_distributions[dist_info.name] = collections.OrderedDict()
        for module_path in dist_info.module_paths:
            if not module_path.is_file():
                continue
            if module_path.name in {'__main__.py', '__version__.py', '__about__.py'}:
                continue
            cls._analyze_file(known_distributions, library_root, dist_info, module_path)

    @classmethod
    def _analyze_file(cls, known_distributions, library_root, dist_info, module_path):
        empty_index = MigrationIndex([])
        relative_path = module_path.relative_to(library_root)
        module_ref = relative_path.as_posix().replace('/', '.')
        for suffix in ('.py', '.__init__'):
            if module_ref.endswith(suffix):
                module_ref = module_ref[: -len(suffix)]
        logger.info(f"Processing module: {module_ref}")
        languages = Languages(empty_index)
        linter = FileLinter(languages, module_path)
        problems = []
        for problem in linter.lint():
            problems.append({'code': problem.code, 'message': problem.message})
        known_distributions[dist_info.name][module_ref] = problems

    def __repr__(self):
        modules = len(self._module_problems)
        libraries = len(self._module_distributions)
        return f"<{self.__class__.__name__}: {modules} modules, {libraries} libraries>"


class DistInfo:
    """represents installed library in dist-info format
    see https://packaging.python.org/en/latest/specifications/binary-distribution-format/
    """

    def __init__(self, path: Path):
        self._path = path

    @cached_property
    def module_paths(self) -> list[Path]:
        files = []
        with Path(self._path, "RECORD").open(encoding=_DEFAULT_ENCODING) as f:
            for line in f.readlines():
                filename = line.split(',')[0]
                if not filename.endswith(".py"):
                    continue
                files.append(self._path.parent / filename)
        return files

    @cached_property
    def _metadata(self):
        with Path(self._path, "METADATA").open(encoding=_DEFAULT_ENCODING) as f:
            return email.message_from_file(f)

    @property
    def name(self):
        name = self._metadata.get('Name', 'unknown')
        return name.lower()

    @property
    def library_names(self) -> list[str]:
        names = []
        for requirement in self._metadata.get_all('Requires-Dist', []):
            library = self._extract_library_name_from_requires_dist(requirement)
            names.append(library)
        return names

    @staticmethod
    def _extract_library_name_from_requires_dist(requirement: str) -> str:
        delimiters = {' ', '@', '<', '>', ';'}
        for i, char in enumerate(requirement):
            if char in delimiters:
                return requirement[:i]
        return requirement

    def __repr__(self):
        return f"<DistInfoPackage {self._path}>"


if __name__ == "__main__":
    logger = get_logger(__file__)  # this only works for __main__
    Whitelist.rebuild(Path.cwd())

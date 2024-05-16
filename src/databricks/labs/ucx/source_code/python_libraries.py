from __future__ import annotations

from pathlib import Path

COMMENTED_OUT_FOR_PR_1685 = """
class SitePackageContainer(SourceContainer):

    def __init__(self, file_loader: FileLoader, site_package: SitePackage):
        self._file_loader = file_loader
        self._site_package = site_package

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        problems: list[DependencyProblem] = []
        for module_path in self._site_package.module_paths:
            maybe = parent.register_dependency(Dependency(self._file_loader, module_path))
            if maybe.problems:
                problems.extend(maybe.problems)
        return problems

    @property
    def paths(self):
        return self._site_package.module_paths

    def __repr__(self):
        return f"<SitePackageContainer {self._site_package}>"
"""

REQUIRES_DIST_PREFIX = "Requires-Dist: "


class DistInfoPackage:
    """
    represents a wheel package installable via pip
    see https://packaging.python.org/en/latest/specifications/binary-distribution-format/
    """
    @classmethod
    def parse(cls, path: Path):
        # not using importlib.metadata because it only works on accessible packages
        # which we can't guarantee since we have our own emulated sys.paths
        with open(Path(path, "METADATA"), encoding="utf-8") as metadata_file:
            lines = metadata_file.readlines()
            lines = filter(lambda line: line.startswith(REQUIRES_DIST_PREFIX), lines)
            library_names = [cls._extract_library_name_from_requires_dist(line[len(REQUIRES_DIST_PREFIX):]) for line in lines]
        with open(Path(path, "RECORD"), encoding="utf-8") as record_file:
            lines = record_file.readlines()
            files = [line.split(',')[0] for line in lines]
            modules = list(filter(lambda line: line.endswith(".py"), files))
        top_levels_path = Path(path, "top_level.txt")
        if top_levels_path.exists():
            with open(top_levels_path, encoding="utf-8") as top_levels_file:
                top_levels = [line.strip() for line in top_levels_file.readlines()]
        else:
            dir_name = path.name
            # strip extension
            dir_name = dir_name[: dir_name.rindex('.')]
            # strip version
            dir_name = dir_name[: dir_name.rindex('-')]
            top_levels = [dir_name]
        return DistInfoPackage(path, top_levels, [Path(module) for module in modules], library_names)

    @classmethod
    def _extract_library_name_from_requires_dist(cls, spec: str) -> str:
        delimiters = { ' ', '@', '<', '>', ';' }
        for i, c in enumerate(spec):
            if c in delimiters:
                return spec[:i]
        return spec

    def __init__(self, dist_info_path: Path, top_levels: list[str], module_paths: list[Path], library_names: list[str]):
        self._dist_info_path = dist_info_path
        self._top_levels = top_levels
        self._module_paths = module_paths
        self._library_names = library_names

    @property
    def top_levels(self) -> list[str]:
        return self._top_levels

    @property
    def module_paths(self) -> list[Path]:
        return [Path(self._dist_info_path.parent, path) for path in self._module_paths]

    @property
    def library_names(self) -> list[str]:
        return self._library_names

    def __repr__(self):
        return f"<DistInfoPackage {self._dist_info_path}>"

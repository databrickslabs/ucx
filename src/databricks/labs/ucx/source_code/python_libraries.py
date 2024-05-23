from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from subprocess import CalledProcessError

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import (
    BaseLibraryResolver,
    DependencyProblem,
    MaybeDependency,
    SourceContainer,
    DependencyGraph,
    Dependency,
    WrappingLoader,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.whitelist import Whitelist


class PipResolver(BaseLibraryResolver):
    # TODO: https://github.com/databrickslabs/ucx/issues/1643
    # TODO: https://github.com/databrickslabs/ucx/issues/1640

    def __init__(
        self, file_loader: FileLoader, white_list: Whitelist, next_resolver: BaseLibraryResolver | None = None
    ) -> None:
        super().__init__(next_resolver)
        self._file_loader = file_loader
        self._white_list = white_list
        self._lib_install_folder = ""

    def with_next_resolver(self, resolver: BaseLibraryResolver) -> PipResolver:
        return PipResolver(self._file_loader, self._white_list, resolver)

    def resolve_library(self, path_lookup: PathLookup, library: Path) -> MaybeDependency:
        library_path = self._locate_library(path_lookup, library)
        if library_path is None:  # not installed yet
            return self._install_library(path_lookup, library)
        dist_info_path = self._locate_dist_info(library_path, library)
        if dist_info_path is None:  # old package style
            problem = DependencyProblem('no-dist-info', f"No dist-info found for {library.name}")
            return MaybeDependency(None, [problem])
        return self._create_dependency(library, dist_info_path)

    def _locate_library(self, path_lookup: PathLookup, library: Path) -> Path | None:
        # start the quick way
        full_path = path_lookup.resolve(library)
        if full_path is not None:
            return full_path
        # maybe the name needs tweaking
        if "-" in library.name:
            name = library.name.replace("-", "_")
            return self._locate_library(path_lookup, Path(name))
        return None

    def _locate_dist_info(self, library_path: Path, library: Path) -> Path | None:
        if not library_path.parent.exists():
            return None
        dist_info_dir = None
        for package in library_path.parent.iterdir():
            if package.name.startswith(library.name) and package.name.endswith(".dist-info"):
                dist_info_dir = package
                break  # TODO: Matching multiple .dist-info's, https://github.com/databrickslabs/ucx/issues/1751
        if dist_info_dir is not None:
            return dist_info_dir
        # maybe the name needs tweaking
        if "-" in library.name:
            name = library.name.replace("-", "_")
            return self._locate_dist_info(library_path, Path(name))
        return None

    def _install_library(self, path_lookup: PathLookup, library: Path) -> MaybeDependency:
        """Pip install library and augment path look-up to resolve the library at import"""
        # invoke pip install via subprocess to install this library into self._lib_install_folder
        venv = self._temporary_virtual_environment(path_lookup).as_posix()
        existing_packages = os.listdir(venv)
        pip_install_arguments = ["pip", "install", library.name, "-t", venv]
        try:
            subprocess.run(pip_install_arguments, check=True)
        except CalledProcessError as e:
            problem = DependencyProblem("library-install-failed", f"Failed to install {library}: {e}")
            return MaybeDependency(None, [problem])
        added_packages = set(os.listdir(venv)).difference(existing_packages)
        dist_info_dirs = list(filter(lambda package: package.endswith(".dist-info"), added_packages))
        dist_info_dir = next((dir for dir in dist_info_dirs if dir.startswith(library.name)), None)
        if dist_info_dir is None and "-" in library.name:
            name = library.name.replace("-", "_")
            dist_info_dir = next((dir for dir in dist_info_dirs if dir.startswith(name)), None)
        if dist_info_dir is None:
            # TODO handle other distribution types
            problem = DependencyProblem('no-dist-info', f"No dist-info found for {library.name}")
            return MaybeDependency(None, [problem])
        dist_info_path = path_lookup.resolve(Path(dist_info_dir))
        assert dist_info_path is not None
        return self._create_dependency(library, dist_info_path)

    def _create_dependency(self, library: Path, dist_info_path: Path):
        package = DistInfoPackage.parse(dist_info_path)
        container = DistInfoContainer(self._file_loader, self._white_list, package)
        dependency = Dependency(WrappingLoader(container), library)
        return MaybeDependency(dependency, [])

    def _temporary_virtual_environment(self, path_lookup: PathLookup) -> Path:
        # TODO: for `databricks labs ucx lint-local-code`, detect if we already have a virtual environment
        # and use that one. See Databricks CLI code for the labs command to see how to detect the virtual
        # environment. If we don't have a virtual environment, create a temporary one.
        # simulate notebook-scoped virtual environment
        if len(self._lib_install_folder) == 0:
            self._lib_install_folder = tempfile.mkdtemp(prefix="ucx-python-libs-")
            path_lookup.append_path(Path(self._lib_install_folder))
        return Path(self._lib_install_folder)


class DistInfoContainer(SourceContainer):

    def __init__(self, file_loader: FileLoader, white_list: Whitelist, package: DistInfoPackage):
        self._file_loader = file_loader
        self._white_list = white_list
        self._package = package

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        problems: list[DependencyProblem] = []
        for module_path in self._package.module_paths:
            maybe = parent.register_dependency(Dependency(self._file_loader, module_path))
            if maybe.problems:
                problems.extend(maybe.problems)
        for library_name in self._package.library_names:
            compatibility = self._white_list.compatibility(library_name)
            if compatibility is not None:
                # TODO attach compatibility to dependency, see https://github.com/databrickslabs/ucx/issues/1382
                continue
            more_problems = parent.register_library(library_name)
            problems.extend(more_problems)
        return problems

    def __repr__(self):
        return f"<DistInfoContainer {self._package}>"


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
            requirements = filter(lambda line: line.startswith(REQUIRES_DIST_PREFIX), lines)
            library_names = [
                cls._extract_library_name_from_requires_dist(requirement[len(REQUIRES_DIST_PREFIX) :])
                for requirement in requirements
            ]
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
        return DistInfoPackage(path, top_levels, [Path(module) for module in modules], list(library_names))

    @classmethod
    def _extract_library_name_from_requires_dist(cls, requirement: str) -> str:
        delimiters = {' ', '@', '<', '>', ';'}
        for i, char in enumerate(requirement):
            if char in delimiters:
                return requirement[:i]
        return requirement

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

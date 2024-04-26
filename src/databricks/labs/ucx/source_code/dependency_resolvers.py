from __future__ import annotations

import abc
import typing
from collections.abc import Callable, Iterable

from pathlib import Path

from databricks.labs.ucx.source_code.dependencies import Dependency, DependencyProblem
from databricks.labs.ucx.source_code.dependency_loaders import SitePackageContainer, WrappingLoader, StubContainer
from databricks.labs.ucx.source_code.site_packages import SitePackages
from databricks.labs.ucx.source_code.syspath_provider import SysPathProvider
from databricks.labs.ucx.source_code.whitelist import Whitelist, UCCompatibility

if typing.TYPE_CHECKING:
    from databricks.labs.ucx.source_code.dependency_loaders import LocalFileLoader, NotebookLoader

# TODO use type alias: type ProblemCollector = Callable[[DependencyProblem], None]


class BaseDependencyResolver(abc.ABC):

    def __init__(self):
        self._next_resolver: DependencyResolver | None = None
        self._problems: list[DependencyProblem] = []

    @property
    def problems(self):
        return self._problems

    def add_problems(self, problems: list[DependencyProblem]):
        self._problems.extend(problems)

    @property
    def next_resolver(self):
        return self._next_resolver

    def set_next_resolver(self, resolver: DependencyResolver):
        self._next_resolver = resolver

    def resolve_notebook(self, path: Path, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        assert self._next_resolver is not None
        return self._next_resolver.resolve_notebook(path, problem_collector)

    def resolve_local_file(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None]
    ) -> Dependency | None:
        assert self._next_resolver is not None
        return self._next_resolver.resolve_local_file(path, problem_collector)

    def resolve_import(self, name: str, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        assert self._next_resolver is not None
        return self._next_resolver.resolve_import(name, problem_collector)


class StubResolver(BaseDependencyResolver):
    def resolve_notebook(self, path: Path, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        return None

    def resolve_local_file(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None]
    ) -> Dependency | None:
        return None

    def resolve_import(self, name: str, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        return None


class NotebookResolver(BaseDependencyResolver):

    def __init__(self, notebook_loader: NotebookLoader):
        super().__init__()
        self._notebook_loader = notebook_loader

    def resolve_notebook(self, path: Path, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        if self._notebook_loader.is_notebook(path):
            return Dependency(self._notebook_loader, path)
        return super().resolve_notebook(path, problem_collector)


class LocalFileResolver(BaseDependencyResolver):

    def __init__(self, file_loader: LocalFileLoader):
        super().__init__()
        self._file_loader = file_loader

    # TODO problem_collector is tactical, pending https://github.com/databrickslabs/ucx/issues/1559
    def resolve_local_file(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None]
    ) -> Dependency | None:
        if self._file_loader.is_file(path) and not self._file_loader.is_notebook(path):
            return Dependency(self._file_loader, path)
        return super().resolve_local_file(path, problem_collector)

    def resolve_import(self, name: str, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        fullpath = self._file_loader.full_path(Path(f"{name}.py"))
        if fullpath is not None:
            return Dependency(self._file_loader, fullpath)
        return super().resolve_import(name, problem_collector)


class WhitelistResolver(BaseDependencyResolver):

    def __init__(self, whitelist: Whitelist):
        super().__init__()
        self._whitelist = whitelist

    # TODO problem_collector is tactical, pending https://github.com/databrickslabs/ucx/issues/1559
    def resolve_import(self, name: str, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        if self._is_whitelisted(name):
            container = StubContainer()
            return Dependency(WrappingLoader(container), Path(name))
        return super().resolve_import(name, problem_collector)

    def _is_whitelisted(self, name: str) -> bool:
        compatibility = self._whitelist.compatibility(name)
        # TODO attach compatibility to dependency, see https://github.com/databrickslabs/ucx/issues/1382
        if compatibility is None:
            return False
        if compatibility == UCCompatibility.NONE:
            # TODO move to linter, see https://github.com/databrickslabs/ucx/issues/1527
            self._problems.append(
                DependencyProblem(
                    code="dependency-check",
                    message=f"Use of dependency {name} is deprecated",
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=0,
                )
            )
        return True


class SitePackagesResolver(BaseDependencyResolver):

    def __init__(self, site_packages: SitePackages, file_loader: LocalFileLoader, syspath_provider: SysPathProvider):
        super().__init__()
        self._site_packages = site_packages
        self._file_loader = file_loader
        self._syspath_provider = syspath_provider

    def resolve_import(self, name: str, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        site_package = self._site_packages[name]
        if site_package is not None:
            container = SitePackageContainer(self._file_loader, site_package, self._syspath_provider)
            return Dependency(WrappingLoader(container), Path(name))
        return super().resolve_import(name, problem_collector)


class DependencyResolver:

    @staticmethod
    def initialize(
        whitelist: Whitelist,
        site_packages: SitePackages,
        file_loader: LocalFileLoader,
        notebook_loader: NotebookLoader,
        syspath_provider: SysPathProvider,
    ) -> DependencyResolver:
        resolver = DependencyResolver()
        resolver.push(NotebookResolver(notebook_loader))
        resolver.push(SitePackagesResolver(site_packages, file_loader, syspath_provider))
        resolver.push(WhitelistResolver(whitelist))
        resolver.push(LocalFileResolver(file_loader))
        return resolver

    def __init__(self):
        self._resolver = StubResolver()

    def push(self, resolver: BaseDependencyResolver):
        resolver.set_next_resolver(self._resolver)
        self._resolver = resolver

    def pop(self) -> BaseDependencyResolver:
        resolver = self._resolver
        self._resolver = resolver.next_resolver
        return resolver

    def resolve_notebook(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None] | None = None
    ) -> Dependency | None:
        problems: list[DependencyProblem] = []
        dependency = self._resolver.resolve_notebook(path, problems.append)
        if dependency is None:
            problem = DependencyProblem('dependency-check', f"Notebook not found: {path.as_posix()}")
            problems.append(problem)
        if problem_collector:
            for problem in problems:
                problem_collector(problem)
        else:
            self.add_problems(problems)
        return dependency

    def resolve_local_file(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None] | None = None
    ) -> Dependency | None:
        problems: list[DependencyProblem] = []
        dependency = self._resolver.resolve_local_file(path, problems.append)
        if dependency is None:
            problem = DependencyProblem('dependency-check', f"File not found: {path.as_posix()}")
            problems.append(problem)
        if problem_collector:
            for problem in problems:
                problem_collector(problem)
        else:
            self.add_problems(problems)
        return dependency

    def resolve_import(
        self, name: str, problem_collector: Callable[[DependencyProblem], None] | None = None
    ) -> Dependency | None:
        problems: list[DependencyProblem] = []
        dependency = self._resolver.resolve_import(name, problems.append)
        if dependency is None:
            problem = DependencyProblem('dependency-check', f"Could not locate import: {name}")
            problems.append(problem)
        if problem_collector:
            for problem in problems:
                problem_collector(problem)
        else:
            self.add_problems(problems)
        return dependency

    @property
    def problems(self) -> Iterable[DependencyProblem]:
        resolver = self._resolver
        while resolver is not None:
            yield from resolver.problems
            resolver = resolver.next_resolver

    def add_problems(self, problems: list[DependencyProblem]):
        self._resolver.add_problems(problems)

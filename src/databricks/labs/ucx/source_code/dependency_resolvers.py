from __future__ import annotations

import abc
import typing
from collections.abc import Callable, Iterable

from pathlib import Path

from databricks.labs.ucx.source_code.dependency_graph import Dependency
from databricks.labs.ucx.source_code.dependency_problem import DependencyProblem
from databricks.labs.ucx.source_code.dependency_loaders import WrappingLoader, StubContainer
from databricks.labs.ucx.source_code.dependency_containers import SitePackageContainer
from databricks.labs.ucx.source_code.site_packages import SitePackages
from databricks.labs.ucx.source_code.syspath_provider import SysPathProvider
from databricks.labs.ucx.source_code.whitelist import Whitelist, UCCompatibility

if typing.TYPE_CHECKING:
    from databricks.labs.ucx.source_code.dependency_loaders import LocalFileLoader, NotebookLoader

# TODO use type alias: type ProblemCollector = Callable[[DependencyProblem], None]


class BaseDependencyResolver(abc.ABC):

    def __init__(self, next_resolver: BaseDependencyResolver | None):
        self._next_resolver = next_resolver
        self._problems: list[DependencyProblem] = []

    @abc.abstractmethod
    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        raise NotImplementedError()

    @property
    def problems(self):
        return self._problems

    def add_problems(self, problems: list[DependencyProblem]):
        self._problems.extend(problems)

    @property
    def next_resolver(self):
        return self._next_resolver

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

    def __init__(self):
        super().__init__(None)

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        raise NotImplementedError("Should never happen!")

    def resolve_notebook(self, path: Path, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        return None

    def resolve_local_file(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None]
    ) -> Dependency | None:
        return None

    def resolve_import(self, name: str, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        return None


class NotebookResolver(BaseDependencyResolver):

    def __init__(self, notebook_loader: NotebookLoader, next_resolver: BaseDependencyResolver | None = None):
        super().__init__(next_resolver)
        self._notebook_loader = notebook_loader

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return NotebookResolver(self._notebook_loader, resolver)

    def resolve_notebook(self, path: Path, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        if self._notebook_loader.is_notebook(path):
            return Dependency(self._notebook_loader, path)
        return super().resolve_notebook(path, problem_collector)


class LocalFileResolver(BaseDependencyResolver):

    def __init__(self, file_loader: LocalFileLoader, next_resolver: BaseDependencyResolver | None = None):
        super().__init__(next_resolver)
        self._file_loader = file_loader

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return LocalFileResolver(self._file_loader, resolver)

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

    def __init__(self, whitelist: Whitelist, next_resolver: BaseDependencyResolver | None = None):
        super().__init__(next_resolver)
        self._whitelist = whitelist

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return WhitelistResolver(self._whitelist, resolver)

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

    def __init__(
        self,
        site_packages: SitePackages,
        file_loader: LocalFileLoader,
        syspath_provider: SysPathProvider,
        next_resolver: BaseDependencyResolver | None = None,
    ):
        super().__init__(next_resolver)
        self._site_packages = site_packages
        self._file_loader = file_loader
        self._syspath_provider = syspath_provider

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return SitePackagesResolver(self._site_packages, self._file_loader, self._syspath_provider, resolver)

    def resolve_import(self, name: str, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        site_package = self._site_packages[name]
        if site_package is not None:
            container = SitePackageContainer(self._file_loader, site_package)
            return Dependency(WrappingLoader(container), Path(name))
        return super().resolve_import(name, problem_collector)


class DependencyResolver:

    @classmethod
    def initialize(
        cls,
        whitelist: Whitelist,
        site_packages: SitePackages,
        file_loader: LocalFileLoader,
        notebook_loader: NotebookLoader,
        syspath_provider: SysPathProvider,
    ) -> DependencyResolver:
        return DependencyResolver(
            [
                NotebookResolver(notebook_loader),
                SitePackagesResolver(site_packages, file_loader, syspath_provider),
                WhitelistResolver(whitelist),
                LocalFileResolver(file_loader),
            ]
        )

    def __init__(self, resolvers: list[BaseDependencyResolver]):
        previous: BaseDependencyResolver = StubResolver()
        for resolver in resolvers:
            resolver = resolver.with_next_resolver(previous)
            previous = resolver
        self._resolver: BaseDependencyResolver = previous

    def resolve_notebook(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None] | None = None
    ) -> Dependency | None:
        problems: list[DependencyProblem] = []
        dependency = self._resolver.resolve_notebook(path, problems.append)
        if dependency is None:
            problem = DependencyProblem('notebook-not-found', f"Notebook not found: {path.as_posix()}")
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
            problem = DependencyProblem('file-not-found', f"File not found: {path.as_posix()}")
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
            problem = DependencyProblem('import-not-found', f"Could not locate import: {name}")
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

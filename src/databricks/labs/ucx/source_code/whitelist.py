from __future__ import annotations

import abc
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from collections.abc import Callable, Iterable
from yaml import load_all as load_yaml, Loader

from databricks.labs.ucx.source_code.files import SysPathProvider

from databricks.labs.ucx.source_code.graph import (
    Dependency,
    WrappingLoader,
    SourceContainer,
    DependencyGraph,
    BaseDependencyResolver,
    DependencyProblem,
)


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


class StubContainer(SourceContainer):

    def build_dependency_graph(self, parent: DependencyGraph, syspath_provider: SysPathProvider) -> None:
        pass


class UCCompatibility(Enum):
    NONE = "none"
    PARTIAL = "partial"
    FULL = "full"

    @classmethod
    def value_of(cls, value: str):
        return next((ucc for ucc in cls if ucc.value == value))


@dataclass(kw_only=True)
class Identifier:
    name: str
    version: str | None = None

    def __hash__(self):
        return hash((self.name, self.version or "*"))

    def __eq__(self, other):
        return isinstance(other, Identifier) and self.name == other.name and self.version == other.version


@dataclass
class KnownPackage(abc.ABC):
    identifier: Identifier
    top_level: str

    @abc.abstractmethod
    def compatibility_of(self, name: str) -> UCCompatibility:
        raise NotImplementedError()


@dataclass
class PythonBuiltinPackage(KnownPackage):

    def compatibility_of(self, name: str) -> UCCompatibility:
        return UCCompatibility.FULL


@dataclass(kw_only=True)
class PythonPackage:
    name: str
    compatibility: UCCompatibility
    failures: list[str] = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.compatibility, str):
            self.compatibility = UCCompatibility.value_of(self.compatibility)


@dataclass
class PipPackage(KnownPackage):
    packages: dict[str, PythonPackage]

    def compatibility_of(self, name: str) -> UCCompatibility:
        while len(name) > 0:
            package = self.packages.get(name, None)
            if package is not None:
                return package.compatibility
            parts = name.split(".")
            if len(parts) == 1:
                return UCCompatibility.NONE
            name = ".".join(parts[0:-1])
        return UCCompatibility.NONE


class Whitelist:
    @classmethod
    def parse(cls, data: str):
        yamls = load_yaml(data, Loader=Loader)
        # @dataclass(kw_only=True) fails to convert inner structs, so deserialize manually
        pips: list[PipPackage] = []
        for yaml in yamls:
            identifier = Identifier(**yaml['identifier'])
            top_level = yaml['top_level']
            packages = {p.name: p for p in [PythonPackage(**package) for package in yaml['packages']]}
            pips.append(PipPackage(identifier, top_level, packages))
        return Whitelist(pips)

    def __init__(self, pips: Iterable[PipPackage] | None = None):
        python_version = sys.version
        known_packages: list[KnownPackage] = [
            PythonBuiltinPackage(Identifier(**{"name": name, "version": python_version}), name)
            for name in sys.stdlib_module_names
        ]
        if pips is not None:
            known_packages.extend(pips)
        self._known_packages: dict[str, list[KnownPackage]] = {}
        for known in known_packages:
            top_levels: list[str] = known.top_level if isinstance(known.top_level, list) else [known.top_level]
            for top_level in top_levels:
                packs = self._known_packages.get(top_level, None)
                if packs is None:
                    packs = []
                    self._known_packages[top_level] = packs
                packs.append(known)

    def compatibility(self, name: str) -> UCCompatibility | None:
        root = name.split('.')[0]
        packages = self._known_packages.get(root, None)
        if packages is None:
            return None
        # TODO ignore versioning for now, see https://github.com/databrickslabs/ucx/issues/1382
        known_package = packages[0]
        return known_package.compatibility_of(name)

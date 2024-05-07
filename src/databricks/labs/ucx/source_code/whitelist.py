from __future__ import annotations

import abc
import logging
import sys
from dataclasses import dataclass, field
from enum import Enum
from collections.abc import Iterable

from yaml import load_all as load_yaml, Loader

from databricks.labs.ucx.source_code.path_lookup import PathLookup

from databricks.labs.ucx.source_code.graph import (
    SourceContainer,
    DependencyGraph,
    BaseDependencyResolver,
    DependencyProblem,
    MaybeDependency,
)

logger = logging.getLogger(__name__)


class WhitelistResolver(BaseDependencyResolver):

    def __init__(self, whitelist: Whitelist, next_resolver: BaseDependencyResolver | None = None):
        super().__init__(next_resolver)
        self._whitelist = whitelist

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return WhitelistResolver(self._whitelist, resolver)

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        # TODO attach compatibility to dependency, see https://github.com/databrickslabs/ucx/issues/1382
        compatibility = self._whitelist.compatibility(name)
        if compatibility == UCCompatibility.FULL:
            return MaybeDependency(None, [])
        if compatibility == UCCompatibility.NONE:
            # TODO move to linter, see https://github.com/databrickslabs/ucx/issues/1527
            problem = DependencyProblem("dependency-check", f"Use of dependency {name} is deprecated")
            return MaybeDependency(None, [problem])
        if compatibility == UCCompatibility.PARTIAL:
            problem = DependencyProblem("dependency-check", f"Package {name} is only partially supported by UC")
            return MaybeDependency(None, [problem])
        return super().resolve_import(path_lookup, name)


class StubContainer(SourceContainer):

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        return []


class UCCompatibility(Enum):
    UNKNOWN = "unknown"
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

    @classmethod
    def compatible(cls, name: str):
        return cls(
            Identifier(name=name),
            name,
            {
                name: PythonPackage(name=name, compatibility=UCCompatibility.FULL),
            },
        )

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
        known_packages.extend(
            [
                PipPackage.compatible("click"),
                PipPackage.compatible("databricks"),
                PipPackage.compatible("google"),
                PipPackage.compatible("pandas"),
                PipPackage.compatible("pytest"),
                PipPackage.compatible("requests"),
                PipPackage.compatible("sqlglot"),
                PipPackage.compatible("urllib3"),
                PipPackage.compatible("yaml"),
            ]
        )
        self._known_packages: dict[str, list[KnownPackage]] = {}
        for known in known_packages:
            top_levels: list[str] = known.top_level if isinstance(known.top_level, list) else [known.top_level]
            for top_level in top_levels:
                packs = self._known_packages.get(top_level, None)
                if packs is None:
                    packs = []
                    self._known_packages[top_level] = packs
                packs.append(known)

    def compatibility(self, name: str) -> UCCompatibility:
        if not name:
            return UCCompatibility.UNKNOWN
        root = name.split('.')[0]
        packages = self._known_packages.get(root, None)
        if packages is None:
            return UCCompatibility.UNKNOWN
        # TODO ignore versioning for now, see https://github.com/databrickslabs/ucx/issues/1382
        known_package = packages[0]
        return known_package.compatibility_of(name)

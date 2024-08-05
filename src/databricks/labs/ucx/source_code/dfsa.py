from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from databricks.labs.ucx.source_code.graph import DependencyGraph, Dependency


@dataclass
class DFSA:
    """A DFSA is a record describing a Direct File System Access"""
    path: str


class DfsaCollector:
    """DfsaCollector is responsible for collecting and storing DFSAs i.e. Direct File System Access records"""

    def collect(self, graph: DependencyGraph) -> Iterable[DFSA]:
        collected_paths: set[Path] = set()
        for dependency in graph.root_dependencies:
            root = dependency.path  # since it's a root
            yield from self._collect_one(dependency, graph, root, collected_paths)

    def _collect_one(self, dependency: Dependency, graph: DependencyGraph, root_path: Path, collected_paths: set[Path]) -> Iterable[DFSA]:
        if dependency.path in collected_paths:
            return
        collected_paths.add(dependency.path)

        yield from []

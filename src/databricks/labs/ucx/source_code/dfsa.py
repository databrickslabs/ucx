from collections.abc import Iterable
from dataclasses import dataclass

from databricks.labs.ucx.source_code.graph import DependencyGraph


@dataclass
class DFSA:
    """A DFSA is a record describing a Direct File System Access"""
    path: str


class DfsaCollector:
    """DfsaCollector is responsible for collecting and storing DFSAs i.e. Direct File System Access records"""

    def collect(self, graph: DependencyGraph) -> Iterable[DFSA]:
        yield from []

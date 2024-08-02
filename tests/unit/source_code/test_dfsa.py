from pathlib import Path

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.dfsa import DfsaCollector


def test_dfsa_does_not_collect_erroneously(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(Path("leaf4.py"), CurrentSessionState())
    collector = DfsaCollector()
    dfsas = list(collector.collect(maybe.graph))
    assert not dfsas

from pathlib import Path

import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.files import FileLoader, LocalFile
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph


def test_local_file_path_is_accessible() -> None:
    local_file = LocalFile(Path("test.py"), "print(1)", Language.PYTHON)
    assert local_file.path == Path("test.py")


@pytest.mark.parametrize("language", [Language.SQL, Language.SCALA, Language.R])
def test_local_file_builds_dependency_graph_without_problems_independent_from_source(
    simple_dependency_resolver, mock_path_lookup, language: Language
) -> None:
    """Unsupported language and SQL builds a dependency graph without problems"""
    dependency = Dependency(FileLoader(), Path("test.py"))
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    local_file = LocalFile(Path("test.py"), "does not matter", language)
    assert not local_file.build_dependency_graph(graph)

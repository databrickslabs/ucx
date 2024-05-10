from unittest.mock import create_autospec

from databricks.labs.ucx.source_code.graph import DependencyGraph, Dependency


def test_dependency_graph_without_parent_root_is_self(mock_path_lookup):
    """The dependency graph root should be itself when there is no parent."""
    dependency = create_autospec(Dependency)
    graph = DependencyGraph(
        dependency=dependency,  # TODO: Replace autospec with object
        parent=None,
        resolver=None,  # TODO: Replace None with object
        path_lookup=mock_path_lookup
    )
    assert graph.root == graph



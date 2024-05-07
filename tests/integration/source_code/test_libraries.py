from pathlib import Path

from tests.unit import MockPathLookup


def test_loads_pip_library_from_notebook(simple_ctx):
    # this tests uses part of unit testing framework to mock the path lookup,
    # and the only reason it's integration tests because it uses the context
    # and the time it takes to run the test.
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())
    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path('lib'))
    assert not maybe.problems
    relative_paths = maybe.graph.all_relative_names()
    assert 'databricks/labs/blueprint/installer.py' in relative_paths

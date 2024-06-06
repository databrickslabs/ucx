from pathlib import Path

from tests.unit.conftest import MockPathLookup


def test_loads_pip_library_from_notebook(simple_ctx):
    # this tests uses part of unit testing framework to mock the path lookup,
    # and the only reason it's integration tests because it uses the context
    # and the time it takes to run the test.
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())

    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path('install_demo_wheel'))

    assert not maybe.problems
    assert maybe.graph.all_relative_names() == {'install_demo_wheel.py', 'thingy/__init__.py'}

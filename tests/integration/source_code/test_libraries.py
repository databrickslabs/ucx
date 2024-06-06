"""
These tests uses part of unit testing framework to mock the path lookup, and the only reason it"s integration tests
because it uses the context and the time it takes to run the test.
"""

from pathlib import Path

from tests.unit.conftest import MockPathLookup


def test_build_notebook_dependency_graphs_installs_wheel_with_pip_cell_in_notebook(simple_ctx):
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())

    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path("pip_install_demo_wheel"))

    assert not maybe.problems
    assert maybe.graph.all_relative_names() == {"pip_install_demo_wheel.py", "thingy/__init__.py"}


def test_build_notebook_dependency_graphs_installs_pytest_with_pip_cell_in_notebook(simple_ctx):
    # pytest is in whitelist, intent is to make sure there are no problems
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())

    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path("pip_install_pytest"))

    assert len(maybe.problems) == 0

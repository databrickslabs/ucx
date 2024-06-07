"""
These tests uses part of unit testing framework to mock the path lookup, and the only reason it"s integration tests
because it uses the context and the time it takes to run the test.
"""

from pathlib import Path

import pytest

from tests.unit.conftest import MockPathLookup


@pytest.mark.parametrize("notebook", ("pip_install_demo_wheel_and_pytest", ))
def test_build_notebook_dependency_graphs_installs_wheel_with_pip_cell_in_notebook(simple_ctx, notebook):
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())

    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path(notebook))

    assert not maybe.problems
    assert maybe.graph.all_relative_names() == {f"{notebook}.py", "thingy/__init__.py"}


@pytest.mark.xfail(reason="Spaces in path are not handled by subprocess when quoted")
@pytest.mark.parametrize("notebook", ("pip_install_demo_wheel_with_spaces_in_target_directory", ))
def test_build_notebook_dependency_graphs_fails_installing_when_spaces(simple_ctx, notebook):
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())

    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path(notebook))

    assert not maybe.problems
    assert maybe.graph.all_relative_names() == {f"{notebook}.py", "thingy/__init__.py"}

"""
These tests uses part of unit testing framework to mock the path lookup, and the only reason it's integration tests
because it uses the context and the time it takes to run the test.
"""

import logging
import re
from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from tests.unit.conftest import MockPathLookup


@pytest.mark.parametrize(
    "notebook",
    (
        "pip_install_demo_wheel",
        "pip_install_demo_wheel_and_pytest",
        "pip_install_demo_wheel_many_flags",
        "pip_install_demo_wheel_with_target_directory",
    ),
)
def test_build_notebook_dependency_graphs_installs_wheel_with_pip_cell_in_notebook(simple_ctx, notebook):
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())

    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path(notebook), CurrentSessionState())

    assert not maybe.problems
    assert maybe.graph.all_relative_names() == {f"{notebook}.py", "thingy/__init__.py"}


def test_build_notebook_dependency_graphs_installs_pytest_from_index_url(simple_ctx):
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())
    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(
        Path("pip_install_pytest_with_index_url"), CurrentSessionState()
    )
    assert not maybe.problems


def test_build_notebook_dependency_graphs_installs_pypi_packages(simple_ctx):
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())
    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(
        Path("pip_install_multiple_packages"), CurrentSessionState()
    )
    assert not maybe.problems
    assert maybe.graph.path_lookup.resolve(Path("splink"))
    assert maybe.graph.path_lookup.resolve(Path("mlflow"))
    assert maybe.graph.path_lookup.resolve(Path("hyperopt"))


@pytest.mark.parametrize("notebook", ("pip_install_demo_wheel_with_spaces_in_target_directory",))
def test_build_notebook_dependency_graphs_fails_installing_when_spaces(simple_ctx, notebook):
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())

    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path(notebook), CurrentSessionState())

    assert not maybe.problems
    assert maybe.graph.all_relative_names() == {f"{notebook}.py", "thingy/__init__.py"}


def test_build_notebook_dependency_graphs_when_installing_pytest_twice(caplog, simple_ctx) -> None:
    pip_already_exists_warning = re.compile(
        r".*WARNING: Target directory .+ already exists\. Specify --upgrade to force replacement.*"
    )
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())
    with caplog.at_level(logging.DEBUG, logger="databricks.labs.ucx.source_code.python_libraries"):
        maybe = ctx.dependency_resolver.build_notebook_dependency_graph(
            Path("pip_install_pytest_twice"), CurrentSessionState()
        )
    assert not maybe.problems
    assert maybe.graph.path_lookup.resolve(Path("pytest"))
    assert not pip_already_exists_warning.match(caplog.text.replace("\n", " ")), "Pip already exists warning detected"


@pytest.mark.parametrize(
    "notebook",
    (
        "pip_install_demo_wheel",
        "pip_install_multiple_packages",
        "pip_install_pytest_with_index_url",
    ),
)
def test_build_notebook_dependency_graphs_when_installing_notebooks_twice(caplog, simple_ctx, notebook) -> None:
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())
    for _ in range(2):
        maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path(notebook), CurrentSessionState())
        assert not maybe.problems

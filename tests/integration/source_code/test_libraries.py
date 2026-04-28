"""
These tests uses part of unit testing framework to mock the path lookup, and the only reason it's integration tests
because it uses the context and the time it takes to run the test.
"""

import configparser
import logging
import os
import re
import textwrap
from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from tests.unit.conftest import MockPathLookup


def _resolved_index_url() -> str:
    """Return a pip-usable index URL for the current environment.

    The notebook fixture invokes a real `pip install --index-url <url>`. CI's jfrog-auth action
    writes a pip.conf at ``$PIP_CONFIG_FILE`` whose ``[global] index-url`` carries the
    JFrog token; ``UV_INDEX_URL`` exists too but lacks credentials (uv stores auth in its keyring),
    so passing it to pip would 401. Prefer ``PIP_CONFIG_FILE`` so pip can authenticate; fall back
    to ``PIP_INDEX_URL`` and finally to the dev proxy used in local development.
    """
    pip_config_file = os.environ.get("PIP_CONFIG_FILE")
    if pip_config_file and Path(pip_config_file).is_file():
        parser = configparser.ConfigParser()
        parser.read(pip_config_file)
        url = parser.get("global", "index-url", fallback=None)
        if url:
            return url
    return os.environ.get("PIP_INDEX_URL") or "https://pypi-proxy.dev.databricks.com/simple/"


def _write_pytest_with_index_url_notebook(directory: Path) -> str:
    notebook_name = "pip_install_pytest_with_index_url"
    notebook = directory / f"{notebook_name}.py"
    notebook.write_text(
        textwrap.dedent(
            f"""\
            # Databricks notebook source

            # COMMAND ----------

            # MAGIC %pip install pytest --index-url {_resolved_index_url()}

            # COMMAND ----------

            import pytest
            """
        )
    )
    return notebook_name


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


def test_build_notebook_dependency_graphs_installs_pytest_from_index_url(simple_ctx, tmp_path):
    notebook_name = _write_pytest_with_index_url_notebook(tmp_path)
    ctx = simple_ctx.replace(path_lookup=MockPathLookup(cwd=tmp_path))
    maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path(notebook_name), CurrentSessionState())
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
    ),
)
def test_build_notebook_dependency_graphs_when_installing_notebooks_twice(caplog, simple_ctx, notebook) -> None:
    ctx = simple_ctx.replace(path_lookup=MockPathLookup())
    for _ in range(2):
        maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path(notebook), CurrentSessionState())
        assert not maybe.problems


def test_build_notebook_dependency_graphs_when_installing_pytest_from_index_url_twice(simple_ctx, tmp_path) -> None:
    notebook_name = _write_pytest_with_index_url_notebook(tmp_path)
    ctx = simple_ctx.replace(path_lookup=MockPathLookup(cwd=tmp_path))
    for _ in range(2):
        maybe = ctx.dependency_resolver.build_notebook_dependency_graph(Path(notebook_name), CurrentSessionState())
        assert not maybe.problems

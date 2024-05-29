from pathlib import Path
from unittest.mock import create_autospec, call

from databricks.labs.ucx.source_code.graph import DependencyProblem
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import PipResolver
from databricks.labs.ucx.source_code.known import Whitelist


def test_pip_resolver_resolves_library(mock_path_lookup):
    def mock_pip_install(command):
        assert command.startswith("pip install anything -t")
        return 0, "", ""

    pip_resolver = PipResolver(Whitelist(), mock_pip_install)
    problems = pip_resolver.register_library(mock_path_lookup, Path("anything"))

    assert len(problems) == 0


def test_pip_resolver_failing(mock_path_lookup):
    def mock_pip_install(_):
        return 1, "", "nope"

    pip_resolver = PipResolver(Whitelist(), mock_pip_install)
    problems = pip_resolver.register_library(mock_path_lookup, Path("anything"))

    assert problems == [DependencyProblem("library-install-failed", "Failed to install anything: nope")]


def test_pip_resolver_adds_to_path_lookup_only_once():
    def mock_pip_install(_):
        return 0, "", ""

    path_lookup = create_autospec(PathLookup)
    pip_resolver = PipResolver(Whitelist(), mock_pip_install)

    problems = pip_resolver.register_library(path_lookup, Path("library"))
    assert len(problems) == 0
    problems = pip_resolver.register_library(path_lookup, Path("library2"))
    assert len(problems) == 0

    venv = pip_resolver._temporary_virtual_environment  # pylint: disable=protected-access
    path_lookup.append_path.assert_has_calls([call(venv), call(venv)])


def test_pip_resolver_resolves_library_with_known_problems(mock_path_lookup):
    def mock_pip_install(_):
        return 0, "", ""

    pip_resolver = PipResolver(Whitelist(), mock_pip_install)
    problems = pip_resolver.register_library(mock_path_lookup, Path("boto3==1.17.0"))

    assert len(problems) == 1
    assert problems[0].code == "direct-filesystem-access"

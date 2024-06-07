from unittest.mock import create_autospec, call

from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from databricks.labs.ucx.source_code.known import Whitelist


def test_python_library_resolver_resolves_library(mock_path_lookup):
    def mock_pip_install(command):
        assert command.startswith("pip install anything -t")
        return 0, "", ""

    python_library_resolver = PythonLibraryResolver(Whitelist(), mock_pip_install)
    problems = python_library_resolver.register_library(mock_path_lookup, "anything")

    assert len(problems) == 0


def test_python_library_resolver_failing(mock_path_lookup):
    def mock_pip_install(_):
        return 1, "", "nope"

    python_library_resolver = PythonLibraryResolver(Whitelist(), mock_pip_install)
    problems = python_library_resolver.register_library(mock_path_lookup, "anything")

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("'pip install anything")
    assert problems[0].message.endswith("nope'")


def test_python_library_resolver_adds_to_path_lookup_only_once():
    def mock_pip_install(_):
        return 0, "", ""

    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = None
    python_library_resolver = PythonLibraryResolver(Whitelist(), mock_pip_install)

    problems = python_library_resolver.register_library(path_lookup, "library")
    assert len(problems) == 0
    problems = python_library_resolver.register_library(path_lookup, "library2")
    assert len(problems) == 0

    venv = python_library_resolver._temporary_virtual_environment  # pylint: disable=protected-access
    path_lookup.append_path.assert_has_calls([call(venv), call(venv)])


def test_python_library_resolver_resolves_library_with_known_problems(mock_path_lookup):
    def mock_pip_install(_):
        return 0, "", ""

    python_library_resolver = PythonLibraryResolver(Whitelist(), mock_pip_install)
    problems = python_library_resolver.register_library(mock_path_lookup, "boto3==1.17.0")

    assert len(problems) == 1
    assert problems[0].code == "direct-filesystem-access"


def test_python_library_resolver_installs_with_command(mock_path_lookup):
    def mock_pip_install(_):
        return 0, "", ""

    python_library_resolver = PythonLibraryResolver(Whitelist(), mock_pip_install)
    problems = python_library_resolver.register_library(
        mock_path_lookup, "library.whl", installation_arguments=["library.whl", "--verbose"]
    )

    assert len(problems) == 0


def test_python_library_resolver_warns_when_install_command_misses_library(mock_path_lookup):
    def mock_pip_install(_):
        return 0, "", ""

    python_library_resolver = PythonLibraryResolver(Whitelist(), mock_pip_install)
    problems = python_library_resolver.register_library(
        mock_path_lookup, "library.whl", installation_arguments=["other_library.whl", "--verbose"]
    )

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith(
        "Missing libraries 'library.whl' in installation command 'pip install other_library.whl --verbose"
    )

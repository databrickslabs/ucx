from unittest.mock import create_autospec, call

from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from databricks.labs.ucx.source_code.known import KnownList


def test_python_library_resolver_resolves_library(mock_path_lookup) -> None:
    def mock_pip_install(command):
        command_str = command if isinstance(command, str) else " ".join(command)
        assert command_str.startswith("pip --disable-pip-version-check install anything -t")
        return 0, "", ""

    python_library_resolver = PythonLibraryResolver(KnownList(), mock_pip_install)
    problems = python_library_resolver.register_library(mock_path_lookup, "anything")

    assert len(problems) == 0


def test_python_library_resolver_failing(mock_path_lookup) -> None:
    def mock_pip_install(_):
        return 1, "", "nope"

    python_library_resolver = PythonLibraryResolver(KnownList(), mock_pip_install)
    problems = python_library_resolver.register_library(mock_path_lookup, "anything")

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("'pip --disable-pip-version-check install anything")
    assert problems[0].message.endswith("nope'")


def test_python_library_resolver_adds_to_path_lookup_only_once() -> None:
    def mock_pip_install(_):
        return 0, "", ""

    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = None
    python_library_resolver = PythonLibraryResolver(KnownList(), mock_pip_install)

    problems = python_library_resolver.register_library(path_lookup, "library")
    assert len(problems) == 0
    problems = python_library_resolver.register_library(path_lookup, "library2")
    assert len(problems) == 0

    venv = python_library_resolver._temporary_virtual_environment  # pylint: disable=protected-access
    path_lookup.append_path.assert_has_calls([call(venv), call(venv)])


def test_python_library_resolver_resolves_library_with_known_problems(mock_path_lookup) -> None:
    def mock_pip_install(_):
        return 0, "", ""

    python_library_resolver = PythonLibraryResolver(KnownList(), mock_pip_install)
    problems = python_library_resolver.register_library(mock_path_lookup, "boto3==1.17.0")

    assert len(problems) == 1
    assert problems[0].code == "direct-filesystem-access"


def test_python_library_resolver_installs_with_command(mock_path_lookup) -> None:
    def mock_pip_install(_):
        return 0, "", ""

    python_library_resolver = PythonLibraryResolver(KnownList(), mock_pip_install)
    problems = python_library_resolver.register_library(mock_path_lookup, "library.whl", "--verbose")

    assert len(problems) == 0


def test_python_library_resolver_installs_multiple_eggs(mock_path_lookup) -> None:
    python_library_resolver = PythonLibraryResolver(KnownList())
    problems = python_library_resolver.register_library(mock_path_lookup, "first.egg", "second.egg")

    assert len(problems) == 2
    assert all(problem.code == "library-install-failed" for problem in problems)
    assert "first.egg" in problems[0].message
    assert "second.egg" in problems[1].message

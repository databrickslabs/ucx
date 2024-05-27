from pathlib import Path
from unittest.mock import create_autospec

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import DependencyProblem
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import PipResolver
from databricks.labs.ucx.source_code.known import Whitelist, DistInfo
from tests.unit import locate_site_packages


def test_pip_resolver_resolves_library(mock_path_lookup):
    pip_resolver = PipResolver(FileLoader(), Whitelist())
    maybe = pip_resolver.resolve_library(mock_path_lookup, Path("demo-egg"))  # installs pkgdir

    assert len(maybe.problems) == 0
    assert mock_path_lookup.resolve(Path("pkgdir")).exists()


def test_pip_resolver_does_not_resolve_unknown_library(mock_path_lookup):
    pip_resolver = PipResolver(FileLoader(), Whitelist())
    maybe = pip_resolver.resolve_library(mock_path_lookup, Path("unknown-library-name"))

    assert len(maybe.problems) == 1
    assert maybe.problems[0].code == "library-install-failed"
    assert maybe.problems[0].message.startswith("Failed to install unknown-library-name")
    assert mock_path_lookup.resolve(Path("unknown-library-name")) is None


def test_pip_resolver_locates_dist_info_without_parent():
    mock_path_lookup = create_autospec(PathLookup)
    mock_path_lookup.resolve.return_value = Path("/non/existing/path/")

    pip_resolver = PipResolver(FileLoader(), Whitelist())
    maybe = pip_resolver.resolve_library(mock_path_lookup, Path("library"))

    assert len(maybe.problems) == 1
    assert maybe.problems[0] == DependencyProblem("no-dist-info", "No dist-info found for library")
    mock_path_lookup.resolve.assert_called_once()


def test_pip_resolver_does_not_resolve_already_installed_library_without_dist_info():
    path_lookup = PathLookup.from_sys_path(Path.cwd())
    pip_resolver = PipResolver(FileLoader(), Whitelist())
    maybe = pip_resolver.resolve_library(path_lookup, Path("xdist"))
    assert maybe.dependency is None


def test_pip_resolver_resolves_library_with_hyphen():
    path_lookup = PathLookup.from_sys_path(Path.cwd())
    pip_resolver = PipResolver(FileLoader(), Whitelist())
    maybe = pip_resolver.resolve_library(path_lookup, Path("pyasn1-modules"))
    assert maybe.dependency is not None

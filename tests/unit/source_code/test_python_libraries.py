from pathlib import Path

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import DistInfoPackage, PipResolver
from databricks.labs.ucx.source_code.whitelist import Whitelist
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


def test_dist_info_package_parses_installed_package_with_toplevel():
    site_packages_path = locate_site_packages()
    astroid_path = Path(site_packages_path, "astroid-3.1.0.dist-info")
    package = DistInfoPackage.parse(astroid_path)
    assert "astroid" in package.top_levels
    assert Path(site_packages_path, "astroid", "constraint.py") in package.module_paths
    assert "typing-extensions" in package.library_names


def test_dist_info_package_parses_installed_package_without_toplevel():
    site_packages_path = locate_site_packages()
    astroid_path = Path(site_packages_path, "ruff-0.3.7.dist-info")
    package = DistInfoPackage.parse(astroid_path)
    assert "ruff" in package.top_levels
    assert Path(site_packages_path, "ruff", "__init__.py") in package.module_paths


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

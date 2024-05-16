from pathlib import Path

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.python_libraries import DistInfoPackage, PipResolver
from databricks.labs.ucx.source_code.whitelist import Whitelist
from tests.unit import locate_site_packages


def test_pip_resolver_resolves_library(mock_path_lookup):
    pip_resolver = PipResolver(FileLoader(), Whitelist())
    maybe = pip_resolver.resolve_library(mock_path_lookup, Path("demo-egg"))  # installs pkgdir

    assert len(maybe.problems) == 0
    assert mock_path_lookup.resolve(Path("pkgdir")).exists()


def test_pip_resolver_does_not_resolve_unknown_library(mock_path_lookup):
    library_names = PipResolver(FileLoader(), Whitelist())
    maybe = library_names.resolve_library(mock_path_lookup, Path("unknown-library-name"))

    assert len(maybe.problems) == 1
    assert maybe.problems[0].code == "library-install-failed"
    assert maybe.problems[0].message.startswith("Failed to install unknown-library-name")
    assert mock_path_lookup.resolve(Path("unknown-library-name")) is None


def test_parses_installed_dist_info_package():
    site_packages_path = locate_site_packages()
    astroid_path = Path(site_packages_path, "astroid-3.1.0.dist-info")
    package = DistInfoPackage.parse(astroid_path)
    assert "astroid" in package.top_levels
    assert Path(site_packages_path, "astroid", "constraint.py") in package.module_paths
    assert "typing-extensions" in package.library_names

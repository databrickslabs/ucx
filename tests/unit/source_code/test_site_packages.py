from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import DependencyProblem
from databricks.labs.ucx.source_code.site_packages import PipResolver, SitePackages
from tests.unit import locate_site_packages


def test_pip_resolver_resolve_library(mock_path_lookup):
    """Verify pytest can be resolved"""
    pip_resolver = PipResolver(FileLoader())
    maybe = pip_resolver.resolve_library(mock_path_lookup, "pytest")

    assert len(maybe.problems) == 0
    assert maybe.dependency.path.as_posix() == "pytest"


def test_pip_resolver_resolve_library_unknown_library(mock_path_lookup):
    """Verify installing unknown library."""
    pip_resolver = PipResolver(FileLoader())
    maybe = pip_resolver.resolve_library(mock_path_lookup, "unknown-library-name")

    assert len(maybe.problems) == 1
    assert maybe.problems[0] == DependencyProblem("library-install-failed", "Failed to install unknown-library-name")


def test_reads_site_packages():
    site_packages_path = locate_site_packages()
    site_packages = SitePackages.parse(str(site_packages_path))
    assert site_packages["astroid"] is not None

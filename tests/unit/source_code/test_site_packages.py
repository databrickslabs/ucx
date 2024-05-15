from pathlib import Path

from databricks.labs.ucx.source_code.site_packages import PipResolver, SitePackages
from tests.unit import locate_site_packages


def test_pip_resolver_library(mock_path_lookup):
    pip_installer = PipResolver()
    maybe = pip_installer.resolve_library(mock_path_lookup, "pytest")

    assert len(maybe.problems) == 0
    assert mock_path_lookup.resolve(Path("pytest")).exists()


def test_pip_resolver_unknown_library(mock_path_lookup):
    pip_installer = PipResolver()
    maybe = pip_installer.resolve_library(mock_path_lookup, "unknown-library-name")

    assert len(maybe.problems) == 1
    assert maybe.problems[0].code == "library-install-failed"
    assert maybe.problems[0].message.startswith("Failed to install unknown-library-name")
    assert mock_path_lookup.resolve(Path("unknown-library-name")) is None


def test_reads_site_packages():
    site_packages_path = locate_site_packages()
    site_packages = SitePackages.parse(site_packages_path)
    assert site_packages["astroid"] is not None

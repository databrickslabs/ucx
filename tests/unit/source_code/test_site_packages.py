from pathlib import Path

from databricks.labs.ucx.source_code.site_packages import PipInstaller, SitePackages
from tests.unit import locate_site_packages


def test_pip_installer_install_library(mock_path_lookup):
    """Install and resolve pytest"""
    pip_installer = PipInstaller()
    problems = pip_installer.install_library(mock_path_lookup, "pytest")

    assert len(problems) == 0
    assert mock_path_lookup.resolve(Path("pytest")).exists()


def test_pip_installer_install_library_unknown_library(mock_path_lookup):
    """Installing unknown library returns problem"""
    pip_installer = PipInstaller()
    problems = pip_installer.install_library(mock_path_lookup, "unknown-library-name")

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("Failed to install unknown-library-name")
    assert mock_path_lookup.resolve(Path("unknown-library-name")) is None


def test_reads_site_packages():
    site_packages_path = locate_site_packages()
    site_packages = SitePackages.parse(site_packages_path)
    assert site_packages["astroid"] is not None

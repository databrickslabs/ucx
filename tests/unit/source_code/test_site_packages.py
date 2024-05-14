from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import DependencyProblem
from databricks.labs.ucx.source_code.site_packages import PipResolver, SitePackages, SitePackage
from databricks.labs.ucx.mixins.fixtures import make_random
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


@pytest.fixture
def site_package_path_without_top_level(tmp_path, make_random) -> Path:
    """Mock a site package"""
    package_path = tmp_path / f"ucx-{make_random()}.dist-info"
    package_path.mkdir(parents=True, exist_ok=True)
    record_file = package_path / "RECORD"
    records = (
        "ucx-0.13.13.dist-info/AUTHORS,sha256=6Xr,236",
        "ucx-0.13.13.dist-info/COPYING.txt,sha256=Tutb,1492",
        "ucx-0.13.13.dist-info/INSTALLER,sha256=zuuu,4",
        "ucx-0.13.13.dist-info/METADATA,sha256=s5kX,14421",
        "ucx-0.13.13.dist-info/RECORD,,",
        "ucx-0.13.13.dist-info/REQUESTED,sha256=47DE,0",
        "ucx-0.13.13.dist-info/WHEEL,sha256=2wepM,92",
        "ucx-0.13.13.dist-info/entry_points.txt,sha256=6Xet,45",
        "ucx-0.13.13.dist-info/top_level.txt,sha256=Oywf,5",
        "ucx-0.13.13.dist-info/zip-safe,sha256=AbpH,1",
        "ucx/__init__.py,sha256=u616,507",
        "ucx/__main__.py,sha256=HIRU,10773",
        "ucx/__pycache__/__init__.cpython-310.pyc,,",
        "ucx/__pycache__/__main__.cpython-310.pyc,,",
    )
    record_file.write_text("\n".join(records))
    return package_path


def test_site_package_parse(site_package_path_without_top_level):
    """Parse a mock site package"""
    site_package = SitePackage.parse(site_package_path_without_top_level)
    assert site_package.top_levels == ["ucx"]
    assert site_package._dist_info_path == site_package_path_without_top_level


@pytest.fixture
def site_package_path_with_top_level(site_package_path_without_top_level) -> Path:
    """Mock a site package with top level"""
    top_level_file = site_package_path_without_top_level / "top_level.txt"
    top_level_modules = ("ucx", "databricks_labs")
    top_level_file.write_text("\n".join(top_level_modules))
    return site_package_path_without_top_level


def test_site_package_parse(site_package_path_with_top_level):
    """Parse a mock site package"""
    site_package = SitePackage.parse(site_package_path_with_top_level)
    assert site_package.top_levels == ["ucx", "databricks_labs"]
    assert site_package._dist_info_path == site_package_path_with_top_level


def test_site_packages_parse(site_package_path_without_top_level):
    """Parse mock site packages"""
    site_packages = SitePackages.parse(site_package_path_without_top_level.parent)
    assert len(site_packages._packages) == 1


def test_site_packages_get(site_package_path_without_top_level):
    """Get the ucx site package"""
    site_packages = SitePackages.parse(site_package_path_without_top_level.parent)
    ucx = site_packages["ucx"]
    assert ucx.top_levels == ["ucx"]

from pathlib import Path

from databricks.labs.ucx.source_code.python_libraries import DistInfoPackage
from tests.unit import locate_site_packages


def test_parses_dist_info_package():
    site_packages_path = locate_site_packages()
    astroid_path = Path(site_packages_path, "astroid-3.1.0.dist-info")
    package = DistInfoPackage.parse(astroid_path)
    assert "astroid" in package.top_levels
    assert Path(site_packages_path, "astroid", "constraint.py") in package.module_paths
    assert "typing-extensions" in package.library_names

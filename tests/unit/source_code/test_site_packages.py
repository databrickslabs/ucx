from databricks.labs.ucx.source_code.site_packages import SitePackages
from tests.unit import locate_site_packages


def test_reads_site_packages():
    site_packages_path = locate_site_packages()
    site_packages = SitePackages.parse(site_packages_path)
    assert site_packages["astroid"] is not None

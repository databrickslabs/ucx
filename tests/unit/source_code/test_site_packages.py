import os
from pathlib import Path

from databricks.labs.ucx.source_code.site_packages import SitePackages


def test_reads_site_packages():
    project_path = Path(os.path.dirname(__file__)).parent.parent.parent
    python_lib_path = Path(project_path, ".venv", "lib")
    actual_python = next(file for file in os.listdir(str(python_lib_path)) if file.startswith("python3."))
    site_packages_path = Path(python_lib_path, actual_python, "site-packages")
    site_packages = SitePackages.parse(str(site_packages_path))
    assert site_packages["astroid"] is not None

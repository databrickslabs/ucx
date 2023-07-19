import sys
from pathlib import Path
from uc_migration_toolkit.utils import get_dbutils


def install_uc_upgrade_package():
    # this library is preinstalled on DBR
    import tomli  # noqa: F401

    # this function is provided in Databricks runtime
    ipython = get_ipython()  # noqa: F821

    project_file = Path("../pyproject.toml").absolute()
    dependency_string = " ".join(f"'{d}'" for d in tomli.loads(project_file.read_bytes())['project']['dependencies'])
    ipython.run_line_magic("pip", f"install {dependency_string}")
    get_dbutils().library.restartPython()

    print("Reloading the path-based modules")
    ipython.run_line_magic("load_ext", "autoreload")
    ipython.run_line_magic("autoreload", 2)
    print("Path-based modules successfully reloaded")

    project_root = Path(".").absolute().parent
    print(f"appending the library from {project_root}")
    sys.path.append(str(project_root))

    print("Verifying that package can be properly loaded")
    try:
        from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit  # noqa: F401

        print("Successfully loaded the uc-upgrade package")
    except Exception as e:
        print(
            "Unable to import the UC migration utilities package from source. "
            "Please check that you've imported the whole repository and not just copied one file."
        )
        print("Also check that you have the Files in Repos activated, e.g. use DBR 11.X+")
        print("Original exception:")
        print(e)

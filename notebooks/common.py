import sys
from pathlib import Path


def install_uc_upgrade_package():
    ipython = get_ipython()  # noqa: F821

    ipython.run_line_magic("pip", "install '..[dev]' -I")

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

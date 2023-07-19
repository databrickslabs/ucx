import sys
from pathlib import Path


def install_uc_migration_toolkit():
    # these libraries are preinstalled on DBR
    import tomli  # noqa: F401
    from databricks.sdk.runtime import dbutils  # noqa: F401

    # this function is provided in Databricks runtime
    ipython = get_ipython()  # noqa: F821

    project_file = Path("../pyproject.toml").absolute()
    dependency_string = " ".join(f"'{d}'" for d in tomli.loads(project_file.read_text())['project']['dependencies'])
    ipython.run_line_magic("pip", f"install {dependency_string}")
    dbutils.library.restartPython()

    print("Reloading the path-based modules")
    ipython.run_line_magic("load_ext", "autoreload")
    ipython.run_line_magic("autoreload", 2)
    print("Path-based modules successfully reloaded")


def add_project_source_to_path():
    project_root = (Path("..").resolve() / "src").absolute()
    print(f"appending the library from {project_root}")
    sys.path.append(str(project_root))

    print("Verifying that package can be properly loaded")
    try:
        from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit  # noqa: F401
        print(sys.path)
        print("Successfully loaded the uc-migration-toolkit package")
    except Exception as e:
        print(
            "Unable to import the UC migration utilities package from source. "
            "Please check that you've imported the whole repository and not just copied one file."
        )
        print("Also check that you have the Files in Repos activated, e.g. use DBR 11.X+")
        print("Original exception:")
        raise e

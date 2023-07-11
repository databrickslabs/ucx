import sys
from pathlib import Path
from tempfile import NamedTemporaryFile

#from databricks.sdk.runtime import *  # noqa: F403


def install_uc_upgrade_package():
    ipython = get_ipython()  # noqa: F405

    print("Installing poetry for package management")
    ipython.run_line_magic("pip", "install poetry -I")
    print("Poetry successfully installed")
    print("Installing the uc-upgrade package and it's dependencies")

    with NamedTemporaryFile(suffix="-uc-upgrade-requirements.txt") as requirements_file:
        print(f"Writing requirements to file {requirements_file.name}")
        ipython.run_cell_magic("sh", "", f"poetry export --output={requirements_file.name} --without-hashes")
        print("Saved the requirements to a provided file, installing them with pip")
        ipython.run_line_magic("pip", f"install -r {requirements_file.name} -I")
        print("Requirements installed successfully, restarting Python interpreter")
        dbutils.library.restartPython()  # noqa: F405
        print("Python interpreter restarted successfully")

    print("Reloading the path-based modules")
    ipython.run_line_magic("load_ext", "autoreload")
    ipython.run_line_magic("autoreload", 2)
    print("Path-based modules successfully reloaded")

    project_root = Path(".").absolute().parent
    print(f"appending the uc-upgrade library from {project_root}")
    sys.path.append(project_root)

    print("Verifying that package can be properly loaded")
    try:
        from uc_upgrade.group_migration import GroupMigration  # noqa: F401

        print("Successfully loaded the uc-upgrade package")
    except Exception as e:
        print(
            "Unable to import the UC migration utilities package from source. "
            "Please check that you've imported the whole repository and not just copied one file."
        )
        print("Also check that you have the Files in Repos activated, e.g. use DBR 11.X+")
        print("Original exception:")
        print(e)

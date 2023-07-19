from pathlib import Path
import importlib.util
import sys


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

    print("adding module to the system path")
    module_src_path = Path(f"../src")
    module_root_path = (module_src_path / "uc_migration_toolkit")
    module_init_path = (module_src_path / "uc_migration_toolkit/__init__.py")

    for _path in [module_src_path, module_root_path, module_init_path]:
        sys.path.append(str(_path))

    try:
        import uc_migration_toolkit
        from uc_migration_toolkit.config import MigrationConfig
    except ImportError as e:
        print("Failed to import uc_migration_toolkit")
        raise e

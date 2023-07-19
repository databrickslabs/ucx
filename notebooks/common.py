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

    print("Reloading the path-based modules")
    ipython.run_line_magic("load_ext", "autoreload")
    ipython.run_line_magic("autoreload", 2)
    print("Path-based modules successfully reloaded")

    print("adding module to the system path")
    module_name = "uc_migration_toolkit"
    module_path = Path(f"../src/{module_name}/__init__.py").resolve().absolute()
    spec = importlib.util.spec_from_file_location(module_name, module_path)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Optional; only necessary if you want to be able to import the module
    # by name later.
    sys.modules[module_name] = module

    try:
        import uc_migration_toolkit
        from uc_migration_toolkit.config import MigrationConfig
    except ImportError as e:
        print("Failed to import uc_migration_toolkit")
        raise e

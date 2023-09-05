def pip_install_dependencies():
    # these libraries are preinstalled on DBR
    import tomli
    from databricks.sdk.runtime import dbutils
    from pathlib import Path

    # this function is provided in Databricks runtime
    ipython = get_ipython()  # noqa: F821

    project_file = Path("../pyproject.toml").absolute()
    dependency_string = " ".join(f"'{d}'" for d in tomli.loads(project_file.read_text())["project"]["dependencies"])
    # TODO: switch to wheel
    ipython.run_line_magic("pip", f"install {dependency_string}")
    dbutils.library.restartPython()


def update_module_imports():
    from pathlib import Path

    print("adding databricks.labs.ucx to the system path")
    module_path = str(Path(f"../src/databricks").resolve().absolute())
    __import__("pkgutil").extend_path(module_path, 'databricks')

    import databricks

    databricks.__path__.append(module_path)

    try:
        from databricks.labs.ucx.__about__ import __version__

        print(f'Running UCX v{__version__}')
    except ImportError as e:
        print("Failed to import databricks.labs.ucx")
        raise e

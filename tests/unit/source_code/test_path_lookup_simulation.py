from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.files import ImportFileResolver, FileLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.graph import SourceContainer, DependencyResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from tests.unit import (
    _samples_path,
)


@pytest.mark.parametrize(
    "source, expected",
    [
        (["simulate-sys-path", "siblings", "sibling1_notebook.py"], 2),
        (["simulate-sys-path", "parent-child", "in_parent_folder_notebook.py"], 3),
        (["simulate-sys-path", "child-parent", "child-folder", "in_child_folder_notebook.py"], 3),
        (["simulate-sys-path", "parent-grand-child", "in_parent_folder_notebook.py"], 3),
        (
            [
                "simulate-sys-path",
                "child-grand-parent",
                "child-folder",
                "child-folder",
                "in_grand_child_folder_notebook",
            ],
            3,
        ),
        (["simulate-sys-path", "via-sys-path", "run_notebook_1.py"], 1),
        (["simulate-sys-path", "via-sys-path", "run_notebook_2.py"], 1),
        (["simulate-sys-path", "via-sys-path", "run_notebook_4.py"], 2),
    ],
)
def test_locates_notebooks(source: list[str], expected: int, mock_path_lookup):
    elems = [_samples_path(SourceContainer)]
    elems.extend(source)
    notebook_path = Path(*elems)
    file_loader = FileLoader()
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    allow_list = KnownList()
    import_resolver = ImportFileResolver(file_loader, allow_list)
    pip_resolver = PythonLibraryResolver(allow_list)
    dependency_resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(notebook_path, CurrentSessionState())
    assert not maybe.problems
    assert maybe.graph is not None
    assert len(maybe.graph.all_paths) == expected


@pytest.mark.parametrize(
    "source, expected",
    [
        (["simulate-sys-path", "siblings", "sibling1_file.py"], 2),
        (["simulate-sys-path", "via-sys-path", "import_file_1.py"], 2),
        (["simulate-sys-path", "via-sys-path", "import_file_2.py"], 2),
    ],
)
def test_locates_files(source: list[str], expected: int):
    elems = [_samples_path(SourceContainer)]
    elems.extend(source)
    file_path = Path(*elems)
    allow_list = KnownList()
    lookup = PathLookup.from_sys_path(Path.cwd())
    file_loader = FileLoader()
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(file_loader, allow_list)
    pip_resolver = PythonLibraryResolver(allow_list)
    resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, lookup)
    maybe = resolver.build_local_file_dependency_graph(file_path, CurrentSessionState())
    assert not maybe.problems
    assert maybe.graph is not None
    assert len(maybe.graph.all_dependencies) == expected


def test_locates_notebooks_with_absolute_path():
    with TemporaryDirectory() as parent_dir:
        parent_dir_path = Path(parent_dir)
        child_dir_path = Path(parent_dir_path, "some_folder")
        child_dir_path.mkdir()
        child_file_path = Path(child_dir_path, "some_notebook.py")
        child_file_path.write_text(
            """# Databricks notebook source_code
whatever = 12
""",
            "utf-8",
        )
        parent_file_path = Path(child_dir_path, "run_notebook.py")
        parent_file_path.write_text(
            f"""# Databricks notebook source_code
import sys

sys.path.append('{child_dir_path.as_posix()}')

# COMMAND ----------

# MAGIC %run some_notebook
""",
            "utf-8",
        )
        lookup = PathLookup.from_sys_path(Path.cwd())
        notebook_loader = NotebookLoader()
        notebook_resolver = NotebookResolver(notebook_loader)
        file_loader = FileLoader()
        allow_list = KnownList()
        import_resolver = ImportFileResolver(file_loader, allow_list)
        pip_resolver = PythonLibraryResolver(allow_list)
        resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, lookup)
        maybe = resolver.build_notebook_dependency_graph(parent_file_path, CurrentSessionState())
        assert not maybe.problems
        assert maybe.graph is not None
        assert len(maybe.graph.all_paths) == 2


def test_locates_files_with_absolute_path():
    with TemporaryDirectory() as parent_dir:
        parent_dir_path = Path(parent_dir)
        child_dir_path = Path(parent_dir_path, "some_folder")
        child_dir_path.mkdir()
        child_file_path = Path(child_dir_path, "some_file.py")
        child_file_path.write_text(
            """def stuff():
    pass
""",
            "utf-8",
        )
        parent_file_path = Path(child_dir_path, "import_file.py")
        parent_file_path.write_text(
            f"""# Databricks notebook source

import sys

def func():
    sys.path.append("{child_file_path.as_posix()}")
    from some_file import stuff
    stuff()
""",
            "utf-8",
        )
        lookup = PathLookup.from_sys_path(Path.cwd())
        notebook_loader = NotebookLoader()
        notebook_resolver = NotebookResolver(notebook_loader)
        allow_list = KnownList()
        file_loader = FileLoader()
        import_resolver = ImportFileResolver(file_loader, allow_list)
        pip_resolver = PythonLibraryResolver(allow_list)
        resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, lookup)
        maybe = resolver.build_notebook_dependency_graph(parent_file_path, CurrentSessionState())
        assert not maybe.problems
        assert maybe.graph is not None
        assert maybe.graph.all_relative_names() == {"some_file.py", "import_file.py"}


def test_path_lookup_skips_resolving_within_file_library(tmp_path):
    file = tmp_path / "file.py"
    file.touch()

    lookup = PathLookup(Path.cwd(), [file])

    try:
        lookup.resolve(Path("package.py"))
    except NotADirectoryError:
        assert False, "PathLookup should skip resolving files"
    else:
        assert True


def test_path_lookup_skips_resolving_non_existing_library(tmp_path):
    library = tmp_path / "non-existing-library"

    lookup = PathLookup(Path.cwd(), [library])

    try:
        lookup.resolve(Path("package.py"))
    except FileNotFoundError:
        assert False, "PathLookup should skip resolving non-existing libraries"
    else:
        assert True


def test_path_lookup_resolves_egg_package(tmp_path):
    egg_library = tmp_path / "library.egg"
    egg_library.mkdir()
    (egg_library / "EGG-INFO").touch()

    package_path = egg_library / "package.py"
    package_path.touch()

    lookup = PathLookup(Path.cwd(), [tmp_path])
    resolved_path = lookup.resolve(Path("package.py"))

    assert resolved_path == package_path


def test_path_lookup_does_not_resolve_package_in_corrupt_egg_package(tmp_path):
    # Missing egg-info
    egg_library = tmp_path / "library.egg"
    egg_library.mkdir()

    package_path = egg_library / "package.py"
    package_path.touch()

    lookup = PathLookup(Path.cwd(), [tmp_path])
    resolved_path = lookup.resolve(Path("package.py"))

    assert resolved_path is None


def test_path_lookup_skips_resolving_egg_files(tmp_path):
    egg_library = tmp_path / "library.egg"
    egg_library.touch()

    lookup = PathLookup(Path.cwd(), [tmp_path])

    try:
        lookup.resolve(Path("package.py"))
    except NotADirectoryError:
        assert False, "PathLookup should skip resolving egg files"
    else:
        assert True


def raise_permission_error():
    raise PermissionError("Can't access path")


def test_path_lookup_raises_permission_error_for_path():
    path = create_autospec(Path)
    path.is_absolute.side_effect = raise_permission_error

    lookup = PathLookup(Path.cwd(), [])

    resolved_path = lookup.resolve(path)
    assert resolved_path is None
    path.is_absolute.assert_called_once()


def test_path_lookup_raises_permission_error_for_library_root():
    library_root = create_autospec(Path)
    library_root.is_dir.side_effect = raise_permission_error

    lookup = PathLookup(Path.cwd(), [library_root])

    resolved_path = lookup.resolve(Path("library.py"))
    assert resolved_path is None
    library_root.is_dir.assert_called_once()

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from databricks.labs.ucx.source_code.files import LocalFileResolver
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.graph import SourceContainer, DependencyGraphBuilder, DependencyResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver
from databricks.labs.ucx.source_code.site_packages import SitePackages, SitePackagesResolver
from databricks.labs.ucx.source_code.whitelist import WhitelistResolver, Whitelist
from tests.unit import _samples_path, whitelist_mock, VisitingFileLoader, VisitingNotebookLoader, locate_site_packages


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
        (["simulate-sys-path", "via-sys-path", "run_notebook_1.py"], 2),
        (["simulate-sys-path", "via-sys-path", "run_notebook_2.py"], 2),
        (["simulate-sys-path", "via-sys-path", "run_notebook_3.py"], 2),
        (["simulate-sys-path", "via-sys-path", "run_notebook_4.py"], 2),
    ],
)
def test_locates_notebooks(source: list[str], expected: int):
    visited: dict[str, bool] = {}
    elems = [_samples_path(SourceContainer)]
    elems.extend(source)
    notebook_path = Path(*elems)
    whitelist = Whitelist()
    provider = PathLookup.from_sys_path(Path.cwd())
    file_loader = VisitingFileLoader(provider, visited)
    notebook_loader = VisitingNotebookLoader(provider, visited)
    site_packages = SitePackages.parse(locate_site_packages())
    resolvers = [
        NotebookResolver(notebook_loader),
        SitePackagesResolver(site_packages, file_loader, provider),
        WhitelistResolver(whitelist),
        LocalFileResolver(file_loader),
    ]
    builder = DependencyGraphBuilder(DependencyResolver(resolvers), provider)
    builder.build_notebook_dependency_graph(notebook_path)
    assert len(visited) == expected


@pytest.mark.parametrize(
    "source, expected",
    [
        (["simulate-sys-path", "siblings", "sibling1_file.py"], 2),
        (["simulate-sys-path", "via-sys-path", "import_file_1.py"], 2),
        (["simulate-sys-path", "via-sys-path", "import_file_2.py"], 2),
    ],
)
def test_locates_files(source: list[str], expected: int):
    visited: dict[str, bool] = {}
    elems = [_samples_path(SourceContainer)]
    elems.extend(source)
    file_path = Path(*elems)
    whitelist = whitelist_mock()
    provider = PathLookup.from_sys_path(Path.cwd())
    file_loader = VisitingFileLoader(provider, visited)
    notebook_loader = VisitingNotebookLoader(provider, visited)
    site_packages = SitePackages.parse(locate_site_packages())
    resolvers = [
        NotebookResolver(notebook_loader),
        SitePackagesResolver(site_packages, file_loader, provider),
        WhitelistResolver(whitelist),
        LocalFileResolver(file_loader),
    ]
    builder = DependencyGraphBuilder(DependencyResolver(resolvers), provider)
    builder.build_local_file_dependency_graph(file_path)
    assert len(visited) == expected


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
        visited: dict[str, bool] = {}
        whitelist = Whitelist()
        provider = PathLookup.from_sys_path(Path.cwd())
        file_loader = VisitingFileLoader(provider, visited)
        notebook_loader = VisitingNotebookLoader(provider, visited)
        site_packages = SitePackages.parse(locate_site_packages())
        resolvers = [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whitelist),
            LocalFileResolver(file_loader),
        ]
        builder = DependencyGraphBuilder(DependencyResolver(resolvers), provider)
        builder.build_notebook_dependency_graph(parent_file_path)
        assert len(visited) == 2


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
            f"""import sys


def func():
    sys.path.append("{child_file_path.as_posix()}")
    from some_file import stuff
    stuff()
""",
            "utf-8",
        )
        visited: dict[str, bool] = {}
        whitelist = Whitelist()
        provider = PathLookup.from_sys_path(Path.cwd())
        file_loader = VisitingFileLoader(provider, visited)
        notebook_loader = VisitingNotebookLoader(provider, visited)
        site_packages = SitePackages.parse(locate_site_packages())
        resolvers = [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whitelist),
            LocalFileResolver(file_loader),
        ]
        builder = DependencyGraphBuilder(DependencyResolver(resolvers), provider)
        builder.build_local_file_dependency_graph(parent_file_path)
        assert len(visited) == 2

from pathlib import Path

import pytest
from databricks.labs.ucx.source_code.files import SysPathProvider, LocalFileResolver
from databricks.labs.ucx.source_code.graph import SourceContainer, DependencyGraphBuilder, DependencyResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver
from databricks.labs.ucx.source_code.site_packages import SitePackages, SitePackagesResolver
from databricks.labs.ucx.source_code.whitelist import WhitelistResolver
from tests.unit import _samples_path, whitelist_mock, VisitingFileLoader, VisitingNotebookLoader, locate_site_packages


@pytest.mark.parametrize(
    "source, expected",
    [
        (["simulate-sys-path", "siblings", "sibling1_notebook"], 2),
        (["simulate-sys-path", "parent-child", "in_parent_folder_notebook"], 3),
        (["simulate-sys-path", "child-parent", "child-folder", "in_child_folder_notebook"], 3),
        (["simulate-sys-path", "parent-grand-child", "in_parent_folder_notebook"], 3),
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
    ],
)
def test_locates_notebooks(source: list[str], expected: int):
    visited: dict[str, bool] = {}
    elems = [_samples_path(SourceContainer)]
    elems.extend(source)
    notebook_path = Path(*elems)
    whitelist = whitelist_mock()
    provider = SysPathProvider.from_sys_path()
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


@pytest.mark.parametrize("source, expected", [(["simulate-sys-path", "siblings", "sibling1_file.py"], 2)])
def test_locates_files(source: list[str], expected: int):
    visited: dict[str, bool] = {}
    elems = [_samples_path(SourceContainer)]
    elems.extend(source)
    file_path = Path(*elems)
    whitelist = whitelist_mock()
    provider = SysPathProvider.from_sys_path()
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

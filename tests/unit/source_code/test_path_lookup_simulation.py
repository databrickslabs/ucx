from pathlib import Path

import pytest
from databricks.labs.ucx.source_code.files import LocalFileResolver, FileLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.graph import SourceContainer, DependencyGraphBuilder, DependencyResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, LocalNotebookLoader
from databricks.labs.ucx.source_code.site_packages import SitePackages, SitePackagesResolver
from databricks.labs.ucx.source_code.whitelist import WhitelistResolver
from tests.unit import (
    _samples_path,
    whitelist_mock,
    locate_site_packages,
    MockPathLookup,
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
    ],
)
def test_locates_notebooks(source: list[str], expected: int):
    elems = [_samples_path(SourceContainer)]
    elems.extend(source)
    notebook_path = Path(*elems)
    lookup = MockPathLookup()
    file_loader = FileLoader()
    notebook_loader = LocalNotebookLoader()
    site_packages = SitePackages.parse(locate_site_packages())
    resolvers = [
        NotebookResolver(notebook_loader),
        SitePackagesResolver(site_packages, file_loader, lookup),
        LocalFileResolver(file_loader),
    ]
    dependency_resolver = DependencyResolver(resolvers)
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_notebook_dependency_graph(notebook_path)
    assert not maybe.problems
    assert maybe.graph is not None
    assert len(maybe.graph.all_paths) == expected


@pytest.mark.parametrize("source, expected", [(["simulate-sys-path", "siblings", "sibling1_file.py"], 2)])
def test_locates_files(source: list[str], expected: int):
    elems = [_samples_path(SourceContainer)]
    elems.extend(source)
    file_path = Path(*elems)
    whitelist = whitelist_mock()
    provider = PathLookup.from_sys_path(Path.cwd())
    file_loader = FileLoader()
    notebook_loader = LocalNotebookLoader()
    site_packages = SitePackages.parse(locate_site_packages())
    resolvers = [
        NotebookResolver(notebook_loader),
        SitePackagesResolver(site_packages, file_loader, provider),
        WhitelistResolver(whitelist),
        LocalFileResolver(file_loader),
    ]
    builder = DependencyGraphBuilder(DependencyResolver(resolvers), provider)
    maybe = builder.build_local_file_dependency_graph(file_path)
    assert not maybe.problems
    assert maybe.graph is not None
    assert len(maybe.graph.all_dependencies) == expected

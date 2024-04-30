from pathlib import Path

from databricks.labs.ucx.source_code.dependency_containers import SourceContainer
from databricks.labs.ucx.source_code.dependency_graph import DependencyGraphBuilder
from databricks.labs.ucx.source_code.dependency_resolvers import DependencyResolver
from databricks.labs.ucx.source_code.site_packages import SitePackages
from databricks.labs.ucx.source_code.syspath_provider import SysPathProvider
from tests.unit import _samples_path, whitelist_mock, VisitingFileLoader, VisitingNotebookLoader, locate_site_packages


def test_locates_sibling_notebook():
    visited: dict[str, bool] = {}
    notebook_path = Path(_samples_path(SourceContainer), "walk-sys-path", "siblings", "sibling1_notebook")
    whi = whitelist_mock()
    provider = SysPathProvider.from_sys_path()
    file_loader = VisitingFileLoader(provider, visited)
    notebook_loader = VisitingNotebookLoader(provider, visited)
    site_packages = SitePackages.parse(locate_site_packages())
    builder = DependencyGraphBuilder(
        DependencyResolver.initialize(whi, site_packages, file_loader, notebook_loader, provider), provider
    )
    builder.build_notebook_dependency_graph(notebook_path)
    assert len(visited) == 2


def test_locates_notebook_in_child_folder():
    visited: dict[str, bool] = {}
    notebook_path = Path(_samples_path(SourceContainer), "walk-sys-path", "parent-child", "in_parent_folder_notebook")
    whi = whitelist_mock()
    provider = SysPathProvider.from_sys_path()
    file_loader = VisitingFileLoader(provider, visited)
    notebook_loader = VisitingNotebookLoader(provider, visited)
    site_packages = SitePackages.parse(locate_site_packages())
    builder = DependencyGraphBuilder(
        DependencyResolver.initialize(whi, site_packages, file_loader, notebook_loader, provider), provider
    )
    builder.build_notebook_dependency_graph(notebook_path)
    assert len(visited) == 2


def test_locates_notebook_in_parent_folder():
    visited: dict[str, bool] = {}
    notebook_path = Path(_samples_path(SourceContainer), "walk-sys-path", "child-parent", "child-folder", "in_child_folder_notebook")
    whi = whitelist_mock()
    provider = SysPathProvider.from_sys_path()
    file_loader = VisitingFileLoader(provider, visited)
    notebook_loader = VisitingNotebookLoader(provider, visited)
    site_packages = SitePackages.parse(locate_site_packages())
    builder = DependencyGraphBuilder(
        DependencyResolver.initialize(whi, site_packages, file_loader, notebook_loader, provider), provider
    )
    builder.build_notebook_dependency_graph(notebook_path)
    assert len(visited) == 2

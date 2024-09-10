from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import DependencyResolver, DependencyGraph, WrappingLoader, Dependency
from databricks.labs.ucx.source_code.known import KnownList, Compatibility, UNKNOWN
from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver, LocalFile
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader, NotebookResolver
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver


def test_graph_visits_package_with_recursive_imports():
    # we need a populated KnownList for this test to run in a reasonable time
    # but sqlglot should be unknown since it exhibits the problematic recursion
    class TestKnownList(KnownList):
        def module_compatibility(self, name: str) -> Compatibility:
            if name.startswith("sqlglot"):
                return UNKNOWN
            return super().module_compatibility(name)

    allow_list = TestKnownList()
    library_resolver = PythonLibraryResolver(allow_list)
    notebook_resolver = NotebookResolver(NotebookLoader())
    import_resolver = ImportFileResolver(FileLoader(), allow_list)
    path_lookup = PathLookup.from_sys_path(Path(__file__).parent)
    dependency_resolver = DependencyResolver(
        library_resolver, notebook_resolver, import_resolver, import_resolver, path_lookup
    )
    root_path = Path(__file__).parent.parent.parent / "unit" / "source_code" / "samples" / "import-sqlglot.py"
    assert root_path.is_file()
    container = LocalFile(root_path, "from sqlglot import Expression\n", Language.PYTHON)
    loader = WrappingLoader(container)
    dependency = Dependency(loader, root_path, False)
    graph = DependencyGraph(dependency, None, dependency_resolver, path_lookup, CurrentSessionState())
    container.build_dependency_graph(graph)
    assert len(graph.all_dependencies) > 10
    # visit the graph without a 'visited' set
    roots = graph.root_dependencies
    assert roots

from pathlib import Path

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.mixins.wspath import WorkspacePath
from databricks.labs.ucx.source_code.files import FileLoader, LocalFileResolver
from databricks.labs.ucx.source_code.graph import DependencyGraphBuilder, DependencyResolver
from databricks.labs.ucx.source_code.jobs import WorkflowLinter
from databricks.labs.ucx.source_code.notebooks.loaders import (
    WorkspaceNotebookLoader,
    NotebookResolver,
    LocalNotebookLoader,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.whitelist import Whitelist


def test_job_linter_no_problems(ws, make_job):
    j = make_job()

    file_loader = FileLoader()
    notebook_loader = WorkspaceNotebookLoader(ws)
    resolvers = [
        NotebookResolver(notebook_loader),
        LocalFileResolver(file_loader),
    ]
    dependency_resolver = DependencyResolver(resolvers)
    path_lookup = PathLookup.from_sys_path(Path('/'))
    builder = DependencyGraphBuilder(dependency_resolver, path_lookup)
    whitelist = Whitelist([])
    migration_index = MigrationIndex([])
    job_linter = WorkflowLinter(ws, builder, migration_index, whitelist)

    problems = job_linter.lint_job(j.job_id)

    assert len(problems) == 0


def test_job_linter_some_notebook_graph_with_problems(ws, make_job, make_notebook, make_random):
    entrypoint = WorkspacePath(ws, f"~/linter-{make_random(4)}").expanduser()
    entrypoint.mkdir()

    main_notebook = entrypoint / 'main'
    make_notebook(path=main_notebook, content=b'%run ./second_notebook')
    j = make_job(notebook_path=main_notebook)

    make_notebook(
        path=entrypoint / 'second_notebook',
        content=b"""import some_file
print('hello world')
display(spark.read.parquet("/mnt/something"))
""",
    )

    some_file = entrypoint / 'some_file.py'
    some_file.write_text('display(spark.read.parquet("/mnt/foo/bar"))')

    file_loader = FileLoader()
    notebook_loader = LocalNotebookLoader()
    resolvers = [
        NotebookResolver(notebook_loader),
        LocalFileResolver(file_loader),
    ]
    dependency_resolver = DependencyResolver(resolvers)
    path_lookup = PathLookup.from_sys_path(Path('/'))
    builder = DependencyGraphBuilder(dependency_resolver, path_lookup)
    whitelist = Whitelist([])
    migration_index = MigrationIndex([])
    job_linter = WorkflowLinter(ws, builder, migration_index, whitelist)

    problems = job_linter.lint_job(j.job_id)

    messages = {f'{Path(p.path).relative_to(entrypoint)}:{p.start_line} [{p.code}] {p.message}' for p in problems}
    assert messages == {
        'second_notebook:4 [direct-filesystem-access] The use of default dbfs: references is deprecated: /mnt/something',
        'some_file.py:1 [direct-filesystem-access] The use of default dbfs: references is deprecated: /mnt/foo/bar',
        'some_file.py:1 [dbfs-usage] Deprecated file system path in call to: /mnt/foo/bar',
        'second_notebook:4 [dbfs-usage] Deprecated file system path in call to: /mnt/something',
    }

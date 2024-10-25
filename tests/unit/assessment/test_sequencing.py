import dataclasses
from datetime import datetime
from unittest.mock import create_autospec

from pathlib import Path

from databricks.sdk.service import jobs

from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.jobs import NotebookTask

from databricks.labs.ucx.assessment.sequencing import MigrationSequencer, MigrationStep
from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.mixins.cached_workspace_path import WorkspaceCache
from databricks.labs.ucx.source_code.base import CurrentSessionState, UsedTable, LineageAtom
from databricks.labs.ucx.source_code.graph import DependencyGraph, Dependency
from databricks.labs.ucx.source_code.jobs import WorkflowTask
from databricks.labs.ucx.source_code.linters.files import FileLoader
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


def ownership_factory(user_name: str):
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = user_name
    return lambda record_type: ownership


def test_sequencer_builds_cluster_and_children_from_task(ws, simple_dependency_resolver, mock_path_lookup):
    ws.clusters.get.return_value = ClusterDetails(cluster_name="my-cluster", creator_user_name="John Doe")
    task = jobs.Task(task_key="test-task", existing_cluster_id="cluster-123")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    ws.jobs.get.return_value = job
    dependency = WorkflowTask(ws, task, job)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.assert_not_called()
    sequencer = MigrationSequencer(ws, mock_path_lookup, ownership_factory("John Doe"), used_tables_crawler)
    sequencer.register_workflow_task(task, job, graph)
    steps = list(sequencer.generate_steps())
    step = steps[-1]
    # we don't know the ids of the steps, se let's zero them
    step = dataclasses.replace(step, step_id=0, required_step_ids=[0] * len(step.required_step_ids))
    assert step == MigrationStep(
        step_id=0,
        step_number=3,
        object_type="CLUSTER",
        object_id="cluster-123",
        object_name="my-cluster",
        object_owner="John Doe",
        required_step_ids=[0, 0],
    )


def test_sequencer_builds_steps_from_dependency_graph(ws, simple_dependency_resolver, mock_path_lookup):
    functional = mock_path_lookup.resolve(Path("functional"))
    mock_path_lookup.append_path(functional)
    mock_path_lookup = mock_path_lookup.change_directory(functional)
    notebook_path = Path("grand_parent_that_imports_parent_that_magic_runs_child.py")
    task = jobs.Task(
        task_key="test-task",
        existing_cluster_id="cluster-123",
        notebook_task=NotebookTask(notebook_path=notebook_path.as_posix()),
    )
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    ws.jobs.get.return_value = job
    ws_cache = create_autospec(WorkspaceCache)
    ws_cache.get_workspace_path.side_effect = Path
    dependency = WorkflowTask(ws, task, job, ws_cache)
    container = dependency.load(mock_path_lookup)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    problems = container.build_dependency_graph(graph)
    assert not problems
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.assert_not_called()
    sequencer = MigrationSequencer(ws, mock_path_lookup, ownership_factory("John Doe"), used_tables_crawler)
    sequencer.register_workflow_task(task, job, graph)
    all_steps = list(sequencer.generate_steps())
    # ensure steps have a consistent step_number: TASK > grand-parent > parent > child
    parent_name = "parent_that_magic_runs_child_that_uses_value_from_parent.py"
    steps = [
        next((step for step in all_steps if step.object_name == "_child_that_uses_value_from_parent.py"), None),
        next((step for step in all_steps if step.object_name == parent_name), None),
        next((step for step in all_steps if step.object_name == notebook_path.as_posix()), None),
        next((step for step in all_steps if step.object_type == "TASK"), None),
    ]
    # ensure steps have a consistent step_number
    for i in range(0, len(steps) - 1):
        assert steps[i]
        assert steps[i].step_number < steps[i + 1].step_number


class _DependencyGraph(DependencyGraph):

    def add_dependency(self, graph: DependencyGraph):
        self._dependencies[graph.dependency] = graph


class _MigrationSequencer(MigrationSequencer):

    def visit_graph(self, graph: DependencyGraph):
        graph.visit(self._visit_dependency, None)


def test_sequencer_supports_cyclic_dependencies(ws, simple_dependency_resolver, mock_path_lookup):
    root = Dependency(FileLoader(), Path("root.py"))
    root_graph = _DependencyGraph(root, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    child_a = Dependency(FileLoader(), Path("a.py"))
    child_graph_a = _DependencyGraph(
        child_a, root_graph, simple_dependency_resolver, mock_path_lookup, CurrentSessionState()
    )
    child_b = Dependency(FileLoader(), Path("b.py"))
    child_graph_b = _DependencyGraph(
        child_b, root_graph, simple_dependency_resolver, mock_path_lookup, CurrentSessionState()
    )
    # root imports a and b
    root_graph.add_dependency(child_graph_a)
    root_graph.add_dependency(child_graph_b)
    # a imports b
    child_graph_a.add_dependency(child_graph_b)
    # b imports a (using local import)
    child_graph_b.add_dependency(child_graph_a)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.assert_not_called()
    sequencer = _MigrationSequencer(ws, mock_path_lookup, ownership_factory("John Doe"), used_tables_crawler)
    sequencer.register_dependency(None, root.lineage[-1].object_type, root.lineage[-1].object_id)
    sequencer.visit_graph(root_graph)
    steps = list(sequencer.generate_steps())
    assert len(steps) == 3
    assert steps[2].object_id == "root.py"


def test_sequencer_builds_steps_from_used_tables(ws, simple_dependency_resolver, mock_path_lookup):
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.for_lineage.side_effect = lambda object_type, object_id: (
        []
        if object_id != "/some-folder/some-notebook"
        else [
            UsedTable(
                source_id="/some-folder/some-notebook",
                source_timestamp=datetime.now(),
                source_lineage=[LineageAtom(object_type="NOTEBOOK", object_id="/some-folder/some-notebook")],
                catalog_name="my-catalog",
                schema_name="my-schema",
                table_name="my-table",
                is_read=False,
                is_write=False,
            )
        ]
    )
    sequencer = _MigrationSequencer(ws, mock_path_lookup, ownership_factory("John Doe"), used_tables_crawler)
    sequencer.register_dependency(None, object_type="FILE", object_id="/some-folder/some-file")
    all_steps = list(sequencer.generate_steps())
    assert len(all_steps) == 1
    sequencer.register_dependency(None, object_type="NOTEBOOK", object_id="/some-folder/some-notebook")
    all_steps = list(sequencer.generate_steps())
    assert len(all_steps) == 3
    step = next((step for step in all_steps if step.object_type == "TABLE"), None)
    assert step
    assert step.step_number == 1
    assert step.object_id == "my-catalog.my-schema.my-table"

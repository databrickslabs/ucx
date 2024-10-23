from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from databricks.labs.blueprint.paths import WorkspacePath

from databricks.labs.ucx.assessment.clusters import ClusterOwnership, ClusterInfo
from databricks.labs.ucx.assessment.jobs import JobOwnership, JobInfo
from databricks.labs.ucx.framework.owners import AdministratorLocator, WorkspacePathOwnership
from databricks.labs.ucx.hive_metastore.tables import TableOwnership, Table
from databricks.labs.ucx.source_code.graph import DependencyGraph
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


@dataclass
class MigrationStep:
    step_id: int
    step_number: int
    object_type: str
    object_id: str
    object_name: str
    object_owner: str
    required_step_ids: list[int]

    @property
    def key(self) -> tuple[str, str]:
        return self.object_type, self.object_id


@dataclass
class MigrationNode:
    node_id: int
    object_type: str
    object_id: str
    object_name: str
    object_owner: str

    @property
    def key(self) -> tuple[str, str]:
        return self.object_type, self.object_id

    def as_step(self, step_number: int, required_step_ids: list[int]) -> MigrationStep:
        return MigrationStep(
            step_id=self.node_id,
            step_number=step_number,
            object_type=self.object_type,
            object_id=self.object_id,
            object_name=self.object_name,
            object_owner=self.object_owner,
            required_step_ids=required_step_ids,
        )


class MigrationSequencer:

    def __init__(
        self,
        ws: WorkspaceClient,
        path_lookup: PathLookup,
        admin_locator: AdministratorLocator,
        used_tables_crawler: UsedTablesCrawler,
    ):
        self._ws = ws
        self._path_lookup = path_lookup
        self._admin_locator = admin_locator
        self._used_tables_crawler = used_tables_crawler
        self._last_node_id = 0
        self._nodes: dict[tuple[str, str], MigrationNode] = {}
        self._outgoing: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)

    def register_workflow_task(self, task: jobs.Task, job: jobs.Job, graph: DependencyGraph) -> MigrationNode:
        task_id = f"{job.job_id}/{task.task_key}"
        task_node = self._nodes.get(("TASK", task_id), None)
        if task_node:
            return task_node
        job_node = self.register_workflow_job(job)
        self._last_node_id += 1
        task_node = MigrationNode(
            node_id=self._last_node_id,
            object_type="TASK",
            object_id=task_id,
            object_name=task.task_key,
            object_owner=job_node.object_owner,  # no task owner so use job one
        )
        self._nodes[task_node.key] = task_node
        self._outgoing[task_node.key].add(job_node.key)
        if task.existing_cluster_id:
            cluster_node = self.register_cluster(task.existing_cluster_id)
            if cluster_node:
                self._outgoing[task_node.key].add(cluster_node.key)
                # also make the cluster dependent on the job
                self._outgoing[job_node.key].add(cluster_node.key)
        graph.visit(self._visit_dependency, None)
        return task_node

    def _visit_dependency(self, graph: DependencyGraph) -> bool | None:
        lineage = graph.dependency.lineage[-1]
        parent_node = self._nodes[(lineage.object_type, lineage.object_id)]
        for dependency in graph.local_dependencies:
            lineage = dependency.lineage[-1]
            self.register_dependency(parent_node, lineage.object_type, lineage.object_id)
            # TODO tables and dfsas
        return False

    def register_dependency(self, parent_node: MigrationNode | None, object_type: str, object_id: str) -> MigrationNode:
        dependency_node = self._nodes.get((object_type, object_id), None)
        if not dependency_node:
            dependency_node = self._create_dependency_node(object_type, object_id)
            list(self._register_used_tables_for(dependency_node))
        if parent_node:
            self._outgoing[dependency_node.key].add(parent_node.key)
        return dependency_node

    def _create_dependency_node(self, object_type: str, object_id: str) -> MigrationNode:
        object_name: str = "<ANONYMOUS>"
        _object_owner: str = "<UNKNOWN>"
        if object_type in {"NOTEBOOK", "FILE"}:
            path = Path(object_id)
            for library_root in self._path_lookup.library_roots:
                if not path.is_relative_to(library_root):
                    continue
                object_name = path.relative_to(library_root).as_posix()
                break
            ws_path = WorkspacePath(self._ws, object_id)
            object_owner = WorkspacePathOwnership(self._admin_locator, self._ws).owner_of(ws_path)
        else:
            raise ValueError(f"{object_type} not supported yet!")
        self._last_node_id += 1
        dependency_node = MigrationNode(
            node_id=self._last_node_id,
            object_type=object_type,
            object_id=object_id,
            object_name=object_name,
            object_owner=object_owner,
        )
        self._nodes[dependency_node.key] = dependency_node
        return dependency_node

    def _register_used_tables_for(self, parent_node: MigrationNode) -> Iterable[MigrationNode]:
        if parent_node.object_type not in {"NOTEBOOK", "FILE"}:
            return
        used_tables = self._used_tables_crawler.for_lineage(parent_node.object_type, parent_node.object_id)
        for used_table in used_tables:
            self._last_node_id += 1
            table_node = MigrationNode(
                node_id=self._last_node_id,
                object_type="TABLE",
                object_id=used_table.fullname,
                object_name=used_table.fullname,
                object_owner=TableOwnership(self._admin_locator).owner_of(Table.from_used_table(used_table)),
            )
            self._nodes[table_node.key] = table_node
            self._outgoing[table_node.key].add(parent_node.key)
            yield table_node

    def register_workflow_job(self, job: jobs.Job) -> MigrationNode:
        job_node = self._nodes.get(("WORKFLOW", str(job.job_id)), None)
        if job_node:
            return job_node
        self._last_node_id += 1
        job_name = job.settings.name if job.settings and job.settings.name else str(job.job_id)
        job_node = MigrationNode(
            node_id=self._last_node_id,
            object_type="WORKFLOW",
            object_id=str(job.job_id),
            object_name=job_name,
            object_owner=JobOwnership(self._admin_locator).owner_of(JobInfo.from_job(job)),
        )
        self._nodes[job_node.key] = job_node
        if job.settings and job.settings.job_clusters:
            for job_cluster in job.settings.job_clusters:
                cluster_node = self.register_job_cluster(job_cluster)
                if cluster_node:
                    self._outgoing[job_node.key].add(cluster_node.key)
        return job_node

    def register_job_cluster(self, cluster: jobs.JobCluster) -> MigrationNode | None:
        if cluster.new_cluster:
            return None
        return self.register_cluster(cluster.job_cluster_key)

    def register_cluster(self, cluster_id: str) -> MigrationNode:
        cluster_node = self._nodes.get(("CLUSTER", cluster_id), None)
        if cluster_node:
            return cluster_node
        details = self._ws.clusters.get(cluster_id)
        object_name = details.cluster_name if details and details.cluster_name else cluster_id
        self._last_node_id += 1
        cluster_node = MigrationNode(
            node_id=self._last_node_id,
            object_type="CLUSTER",
            object_id=cluster_id,
            object_name=object_name,
            object_owner=ClusterOwnership(self._admin_locator).owner_of(ClusterInfo.from_cluster_details(details)),
        )
        self._nodes[cluster_node.key] = cluster_node
        # TODO register warehouses and policies
        return cluster_node

    def generate_steps(self) -> Iterable[MigrationStep]:
        """The below algo is adapted from Kahn's topological sort.
        The differences are as follows:
        1) we want the same step number for all nodes with same dependency depth
            so instead of pushing 'leaf' nodes to a queue, we fetch them again once all current 'leaf' nodes are processed
            (these are transient 'leaf' nodes i.e. they only become 'leaf' during processing)
        2) Kahn only supports DAGs but python code allows cyclic dependencies i.e. A -> B -> C -> A is not a DAG
            so when fetching 'leaf' nodes, we relax the 0-incoming-vertex rule in order
            to avoid an infinite loop. We also avoid side effects (such as negative counts).
            This algo works correctly for simple cases, but is not tested on large trees.
        """
        incoming_keys = self._collect_incoming_keys()
        incoming_counts = self._compute_incoming_counts(incoming_keys)
        step_number = 1
        sorted_steps: list[MigrationStep] = []
        while len(incoming_counts) > 0:
            leaf_keys = self._get_leaf_keys(incoming_counts)
            for leaf_key in leaf_keys:
                del incoming_counts[leaf_key]
                sorted_steps.append(
                    self._nodes[leaf_key].as_step(step_number, list(self._required_step_ids(incoming_keys[leaf_key])))
                )
                self._on_leaf_key_processed(leaf_key, incoming_counts)
            step_number += 1
        return sorted_steps

    def _on_leaf_key_processed(self, leaf_key: tuple[str, str], incoming_counts: dict[tuple[str, str], int]):
        for dependency_key in self._outgoing[leaf_key]:
            # prevent re-instantiation of already deleted keys
            if dependency_key not in incoming_counts:
                continue
            # prevent negative count with cyclic dependencies
            if incoming_counts[dependency_key] > 0:
                incoming_counts[dependency_key] -= 1

    def _collect_incoming_keys(self) -> dict[tuple[str, str], set[tuple[str, str]]]:
        result: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)
        for source, outgoing in self._outgoing.items():
            for target in outgoing:
                result[target].add(source)
        return result

    def _required_step_ids(self, required_step_keys: set[tuple[str, str]]) -> Iterable[int]:
        for source_key in required_step_keys:
            yield self._nodes[source_key].node_id

    def _compute_incoming_counts(
        self, incoming: dict[tuple[str, str], set[tuple[str, str]]]
    ) -> dict[tuple[str, str], int]:
        result = defaultdict(int)
        for node_key in self._nodes:
            result[node_key] = len(incoming[node_key])
        return result

    @classmethod
    def _get_leaf_keys(cls, incoming_counts: dict[tuple[str, str], int]) -> Iterable[tuple[str, str]]:
        max_count = 0
        leaf_keys = list(cls._yield_leaf_keys(incoming_counts, max_count))
        # if we're not finding nodes with 0 incoming counts, it's likely caused by cyclic dependencies
        # in which case it's safe to process nodes with a higher incoming count
        while not leaf_keys:
            max_count += 1
            leaf_keys = list(cls._yield_leaf_keys(incoming_counts, max_count))
        return leaf_keys

    @classmethod
    def _yield_leaf_keys(cls, incoming_counts: dict[tuple[str, str], int], max_count: int) -> Iterable[tuple[str, str]]:
        for node_key, incoming_count in incoming_counts.items():
            if incoming_count > max_count:
                continue
            yield node_key

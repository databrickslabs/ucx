from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from databricks.labs.ucx.source_code.graph import DependencyGraph


@dataclass
class MigrationStep:
    step_id: int
    step_number: int
    object_type: str
    object_id: str
    object_name: str
    object_owner: str
    required_step_ids: list[int]


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

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws
        self._last_node_id = 0
        self._nodes: dict[tuple[str, str], MigrationNode] = {}
        self._incoming: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)
        self._outgoing: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)

    def register_workflow_task(self, task: jobs.Task, job: jobs.Job, _graph: DependencyGraph) -> MigrationNode:
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
        self._incoming[job_node.key].add(task_node.key)
        self._outgoing[task_node.key].add(job_node.key)
        if task.existing_cluster_id:
            cluster_node = self.register_cluster(task.existing_cluster_id)
            if cluster_node:
                self._incoming[cluster_node.key].add(task_node.key)
                self._outgoing[task_node.key].add(cluster_node.key)
                # also make the cluster dependent on the job
                self._incoming[cluster_node.key].add(job_node.key)
                self._outgoing[job_node.key].add(cluster_node.key)
        # TODO register dependency graph
        return task_node

    def register_workflow_job(self, job: jobs.Job) -> MigrationNode:
        job_node = self._nodes.get(("JOB", str(job.job_id)), None)
        if job_node:
            return job_node
        self._last_node_id += 1
        job_name = job.settings.name if job.settings and job.settings.name else str(job.job_id)
        job_node = MigrationNode(
            node_id=self._last_node_id,
            object_type="JOB",
            object_id=str(job.job_id),
            object_name=job_name,
            object_owner=job.creator_user_name or "<UNKNOWN>",
        )
        self._nodes[job_node.key] = job_node
        if job.settings and job.settings.job_clusters:
            for job_cluster in job.settings.job_clusters:
                cluster_node = self.register_job_cluster(job_cluster)
                if cluster_node:
                    self._incoming[cluster_node.key].add(job_node.key)
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
            object_owner=object_owner,
        )
        self._nodes[cluster_node.key] = cluster_node
        # TODO register warehouses and policies
        return cluster_node

    def generate_steps(self) -> Iterable[MigrationStep]:
        # algo adapted from Kahn topological sort. The main differences is that
        # we want the same step number for all nodes with same dependency depth
        # so instead of pushing to a queue, we rebuild it once all leaf nodes are processed
        # (these are transient leaf nodes i.e. they only become leaf during processing)
        incoming_counts = self._populate_incoming_counts()
        step_number = 1
        sorted_steps: list[MigrationStep] = []
        while len(incoming_counts) > 0:
            leaf_keys = list(self._get_leaf_keys(incoming_counts))
            for leaf_key in leaf_keys:
                del incoming_counts[leaf_key]
                sorted_steps.append(self._nodes[leaf_key].as_step(step_number, list(self._required_step_ids(leaf_key))))
                for dependency_key in self._outgoing[leaf_key]:
                    incoming_counts[dependency_key] -= 1
            step_number += 1
        return sorted_steps

    def _required_step_ids(self, node_key: tuple[str, str]) -> Iterable[int]:
        for leaf_key in self._incoming[node_key]:
            yield self._nodes[leaf_key].node_id

    def _populate_incoming_counts(self) -> dict[tuple[str, str], int]:
        result = defaultdict(int)
        for node_key in self._nodes:
            result[node_key] = len(self._incoming[node_key])
        return result

    @staticmethod
    def _get_leaf_keys(incoming_counts: dict[tuple[str, str], int]) -> Iterable[tuple[str, str]]:
        for node_key, incoming_count in incoming_counts.items():
            if incoming_count > 0:
                continue
            yield node_key

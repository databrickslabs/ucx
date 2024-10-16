from __future__ import annotations

import itertools
from collections.abc import Iterable
from dataclasses import dataclass, field

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
    required_step_ids: list[int] = field(default_factory=list)


@dataclass
class MigrationNode:
    last_node_id = 0
    node_id: int
    object_type: str
    object_id: str
    object_name: str
    object_owner: str
    required_steps: list[MigrationNode] = field(default_factory=list)

    def generate_steps(self) -> tuple[MigrationStep, Iterable[MigrationStep]]:
        # traverse the nodes using a depth-first algorithm
        # ultimate leaves have a step number of 1
        # use highest required step number + 1 for this step
        highest_step_number = 0
        required_step_ids: list[int] = []
        all_generated_steps: list[Iterable[MigrationStep]] = []
        for required_step in self.required_steps:
            step, generated_steps = required_step.generate_steps()
            highest_step_number = max(highest_step_number, step.step_number)
            required_step_ids.append(step.step_id)
            all_generated_steps.append(generated_steps)
            all_generated_steps.append([step])
        this_step = MigrationStep(
            step_id=self.node_id,
            step_number=highest_step_number + 1,
            object_type=self.object_type,
            object_id=self.object_id,
            object_name=self.object_name,
            object_owner=self.object_owner,
            required_step_ids=required_step_ids,
        )
        return this_step, itertools.chain(*all_generated_steps)

    def find(self, object_type: str, object_id: str) -> MigrationNode | None:
        if object_type == self.object_type and object_id == self.object_id:
            return self
        for step in self.required_steps:
            found = step.find(object_type, object_id)
            if found:
                return found
        return None


class MigrationSequencer:

    def __init__(self):
        self._root = MigrationNode(
            node_id=0, object_type="ROOT", object_id="ROOT", object_name="ROOT", object_owner="NONE"
        )

    def register_workflow_task(self, task: jobs.Task, job: jobs.Job, _graph: DependencyGraph) -> MigrationNode:
        task_id = f"{job.job_id}/{task.task_key}"
        task_node = self._find_node(object_type="TASK", object_id=task_id)
        if task_node:
            return task_node
        job_node = self.register_workflow_job(job)
        MigrationNode.last_node_id += 1
        task_node = MigrationNode(
            node_id=MigrationNode.last_node_id,
            object_type="TASK",
            object_id=task_id,
            object_name=task.task_key,
            object_owner="NONE",
        )  # TODO object_owner
        job_node.required_steps.append(task_node)
        if task.existing_cluster_id:
            cluster_node = self.register_cluster(task.existing_cluster_id)
            cluster_node.required_steps.append(task_node)
            if job_node not in cluster_node.required_steps:
                cluster_node.required_steps.append(job_node)
        # TODO register dependency graph
        return task_node

    def register_workflow_job(self, job: jobs.Job) -> MigrationNode:
        job_node = self._find_node(object_type="JOB", object_id=str(job.job_id))
        if job_node:
            return job_node
        MigrationNode.last_node_id += 1
        job_name = job.settings.name if job.settings and job.settings.name else str(job.job_id)
        job_node = MigrationNode(
            node_id=MigrationNode.last_node_id,
            object_type="JOB",
            object_id=str(job.job_id),
            object_name=job_name,
            object_owner="NONE",
        )  # TODO object_owner
        top_level = True
        if job.settings and job.settings.job_clusters:
            for job_cluster in job.settings.job_clusters:
                cluster_node = self.register_job_cluster(job_cluster)
                if cluster_node:
                    top_level = False
                    cluster_node.required_steps.append(job_node)
        if top_level:
            self._root.required_steps.append(job_node)
        return job_node

    def register_job_cluster(self, cluster: jobs.JobCluster) -> MigrationNode | None:
        if cluster.new_cluster:
            return None
        return self.register_cluster(cluster.job_cluster_key)

    def register_cluster(self, cluster_key: str) -> MigrationNode:
        cluster_node = self._find_node(object_type="CLUSTER", object_id=cluster_key)
        if cluster_node:
            return cluster_node
        MigrationNode.last_node_id += 1
        cluster_node = MigrationNode(
            node_id=MigrationNode.last_node_id,
            object_type="CLUSTER",
            object_id=cluster_key,
            object_name=cluster_key,
            object_owner="NONE",
        )  # TODO object_owner
        # TODO register warehouses and policies
        self._root.required_steps.append(cluster_node)
        return cluster_node

    def generate_steps(self) -> Iterable[MigrationStep]:
        _root_step, generated_steps = self._root.generate_steps()
        unique_steps = self._deduplicate_steps(generated_steps)
        return self._sorted_steps(unique_steps)

    @staticmethod
    def _sorted_steps(steps: Iterable[MigrationStep]) -> Iterable[MigrationStep]:
        # sort by step number, lowest first
        return sorted(steps, key=lambda step: step.step_number)

    @staticmethod
    def _deduplicate_steps(steps: Iterable[MigrationStep]) -> Iterable[MigrationStep]:
        best_steps: dict[int, MigrationStep] = {}
        for step in steps:
            existing = best_steps.get(step.step_id, None)
            # keep the step with the highest step number
            if existing and existing.step_number >= step.step_number:
                continue
            best_steps[step.step_id] = step
        return best_steps.values()

    def _find_node(self, object_type: str, object_id: str) -> MigrationNode | None:
        return self._root.find(object_type, object_id)

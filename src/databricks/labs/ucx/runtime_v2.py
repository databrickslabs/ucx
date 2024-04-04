import dataclasses
import logging
from pathlib import Path

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.workflows import Assessment
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Task, TaskLogger, Workflow, parse_args
from databricks.labs.ucx.hive_metastore.workflows import TableMigration
from databricks.labs.ucx.workspace_access.workflows import GroupMigration


class Workflows:
    _tasks: list[Task] = []
    _workflows: dict[str, Workflow] = {}

    def __init__(self, workflows: list[Workflow]):
        for workflow in workflows:
            self._workflows[workflow.name] = workflow
            for task_definition in workflow.tasks():
                # Add the workflow name to the task definition, because we cannot access
                # the workflow name from the method decorator
                with_workflow = dataclasses.replace(task_definition, workflow=workflow.name)
                self._tasks.append(with_workflow)

    @classmethod
    def all(cls):
        return cls([Assessment(), GroupMigration(), TableMigration()])

    def tasks(self) -> list[Task]:
        # TODO: a method for the installer to get the list of Task instances
        return self._tasks

    def trigger(self, *argv):
        named_parameters = parse_args(*argv)
        config_path = Path(named_parameters["config"])

        ctx = RuntimeContext(named_parameters)
        install_dir = config_path.parent
        task_name = named_parameters.get("task", "not specified")
        workflow = named_parameters.get("workflow", "not specified")
        if task_name not in self._workflows:
            msg = f'task "{task_name}" not found. Valid tasks are: {", ".join(self._workflows.keys())}'
            raise KeyError(msg)
        print(f"UCX v{__version__}")
        workflow = self._workflows[task_name]
        # `{{parent_run_id}}` is the run of entire workflow, whereas `{{run_id}}` is the run of a task
        workflow_run_id = named_parameters.get("parent_run_id", "unknown_run_id")
        job_id = named_parameters.get("job_id", "unknown_job_id")
        with TaskLogger(
            install_dir,
            workflow=workflow.name,
            workflow_id=job_id,
            task_name=task_name,
            workflow_run_id=workflow_run_id,
            log_level=ctx.config.log_level,
        ) as task_logger:
            ucx_logger = logging.getLogger("databricks.labs.ucx")
            ucx_logger.info(f"UCX v{__version__} After job finishes, see debug logs at {task_logger}")
            current_task = getattr(workflow, task_name)
            current_task(ctx)

import logging
import os
import sys
from pathlib import Path

from databricks.sdk.config import with_user_agent_extra

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.workflows import Assessment, Failing
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, parse_args
from databricks.labs.ucx.installer.logs import TaskLogger
from databricks.labs.ucx.hive_metastore.workflows import (
    ScanTablesInMounts,
    TableMigration,
    MigrateTablesInMounts,
    MigrateHiveSerdeTablesInPlace,
    MigrateExternalTablesCTAS,
    ConvertWASBSToADLSGen2,
)
from databricks.labs.ucx.progress.workflows import MigrationProgress
from databricks.labs.ucx.recon.workflows import MigrationRecon
from databricks.labs.ucx.workspace_access.workflows import (
    LegacyGroupMigration,
    PermissionsMigrationAPI,
    RemoveWorkspaceLocalGroups,
    ValidateGroupPermissions,
)

logger = logging.getLogger(__name__)


class Workflows:
    def __init__(self, workflows: list[Workflow]):
        self.workflows: dict[str, Workflow] = {workflow.name: workflow for workflow in workflows}

    @classmethod
    def all(cls):
        return cls(
            [
                Assessment(),
                MigrationProgress(),
                LegacyGroupMigration(),
                TableMigration(),
                MigrateHiveSerdeTablesInPlace(),
                MigrateExternalTablesCTAS(),
                ValidateGroupPermissions(),
                RemoveWorkspaceLocalGroups(),
                ScanTablesInMounts(),
                MigrateTablesInMounts(),
                ConvertWASBSToADLSGen2(),
                PermissionsMigrationAPI(),
                MigrationRecon(),
                Failing(),
            ]
        )

    def trigger(self, *argv):
        named_parameters = parse_args(*argv)
        config_path = Path(named_parameters["config"])
        ctx = RuntimeContext(named_parameters)
        install_dir = config_path.parent
        task_name = named_parameters.get("task", "not specified")
        workflow_name = named_parameters.get("workflow", "not specified")
        attempt = named_parameters.get("attempt", "0")
        if workflow_name not in self.workflows:
            msg = f'workflow "{workflow_name}" not found. Valid workflows are: {", ".join(self.workflows.keys())}'
            raise KeyError(msg)
        print(f"UCX v{__version__}")
        workflow = self.workflows[workflow_name]
        if task_name == "parse_logs":
            return ctx.task_run_warning_recorder.snapshot()
        # both CLI commands and workflow names appear in telemetry under `cmd`
        with_user_agent_extra("cmd", workflow_name)
        # `{{parent_run_id}}` is the run of entire workflow, whereas `{{run_id}}` is the run of a task
        workflow_run_id = named_parameters.get("parent_run_id", "unknown_run_id")
        job_id = named_parameters.get("job_id", "unknown_job_id")
        with TaskLogger(
            install_dir,
            workflow=workflow_name,
            workflow_id=job_id,
            task_name=task_name,
            workflow_run_id=workflow_run_id,
            log_level=ctx.config.log_level,
            attempt=attempt,
        ) as task_logger:
            ucx_logger = logging.getLogger("databricks.labs.ucx")
            ucx_logger.info(f"UCX v{__version__} After job finishes, see debug logs at {task_logger}")
            current_task = getattr(workflow, task_name)
            current_task(ctx)
            return None


def main(*argv):
    if len(argv) == 0:
        argv = sys.argv
    Workflows.all().trigger(*argv)


if __name__ == "__main__":
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        raise SystemExit("Only intended to run in Databricks Runtime")
    main(*sys.argv)

from __future__ import annotations

import datetime as dt
import logging
import os.path
import re
import sys
import webbrowser
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, TYPE_CHECKING

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.labs.lsql.dashboards import Dashboards
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    Aborted,
    AlreadyExists,
    BadRequest,
    Cancelled,
    DataLoss,
    DeadlineExceeded,
    InternalError,
    InvalidParameterValue,
    NotFound,
    OperationFailed,
    PermissionDenied,
    RequestLimitExceeded,
    ResourceAlreadyExists,
    ResourceConflict,
    ResourceDoesNotExist,
    ResourceExhausted,
    TemporarilyUnavailable,
    TooManyRequests,
    Unauthenticated,
    Unknown,
)
from databricks.sdk.retries import retried
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.jobs import Run, RunLifeCycleState, RunResultState
from databricks.sdk.service.workspace import ObjectType

import databricks
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.tasks import Task
from databricks.labs.ucx.installer.logs import PartialLogRecord, parse_logs
from databricks.labs.ucx.installer.mixins import InstallationMixin

# Although we really don't like using TYPE_CHECKING guards, this is the only way to avoid a circular import without
# significant refactoring.
if TYPE_CHECKING:
    from databricks.labs.ucx.runtime import Workflows

logger = logging.getLogger(__name__)

TEST_RESOURCE_PURGE_TIMEOUT = timedelta(hours=1)
TEST_NIGHTLY_CI_RESOURCES_PURGE_TIMEOUT = timedelta(hours=3)  # Buffer for debugging nightly integration test runs
# See https://docs.databricks.com/en/jobs/parameter-value-references.html#supported-value-references
EXTRA_TASK_PARAMS = {
    "job_id": "{{job_id}}",
    "run_id": "{{run_id}}",
    "start_time": "{{job.start_time.iso_datetime}}",
    "attempt": "{{job.repair_count}}",
    "parent_run_id": "{{parent_run_id}}",
}
DEBUG_NOTEBOOK = """# Databricks notebook source
# MAGIC %md
# MAGIC # Debug companion for UCX installation (see [README]({readme_link}))
# MAGIC
# MAGIC Production runs are supposed to be triggered through the following jobs: {job_links}
# MAGIC
# MAGIC **This notebook is overwritten with each UCX update/(re)install.**

# COMMAND ----------

# MAGIC %pip install {remote_wheel}
dbutils.library.restartPython()

# COMMAND ----------

import logging
from pathlib import Path
from databricks.sdk.config import with_user_agent_extra
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext

install_logger()
with_user_agent_extra("cmd", "debug-notebook")
logging.getLogger("databricks").setLevel("DEBUG")

# ctx.<TAB> to see all available objects for you to
named_parameters = dict(config="/Workspace{config_file}")
ctx = RuntimeContext(named_parameters)

print(__version__)
"""

TEST_RUNNER_NOTEBOOK = """# Databricks notebook source
# MAGIC %pip install {remote_wheel}
dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.ucx.runtime import main

main(f'--config=/Workspace{config_file}',
     f'--workflow=' + dbutils.widgets.get('workflow'),
     f'--task=' + dbutils.widgets.get('task'),
     f'--job_id=' + dbutils.widgets.get('job_id'),
     f'--run_id=' + dbutils.widgets.get('run_id'),
     f'--start_time=' + dbutils.widgets.get('start_time'),
     f'--attempt=' + dbutils.widgets.get('attempt'),
     f'--parent_run_id=' + dbutils.widgets.get('parent_run_id'))
"""

EXPORT_TO_EXCEL_NOTEBOOK = """# Databricks notebook source
# MAGIC %md
# MAGIC ##### Exporter of UCX assessment results
# MAGIC ##### Instructions:
# MAGIC 1. Execute using an all-purpose cluster with Databricks Runtime 14 or higher.
# MAGIC 1. Hit **Run all** button and wait for completion.
# MAGIC 1. Go to the bottom of the notebook and click the Download UCX Results button.
# MAGIC
# MAGIC ##### Important:
# MAGIC Please note that this is only meant to serve as example code.
# MAGIC
# MAGIC Example code developed by **Databricks Shared Technical Services team**.

# COMMAND ----------

# DBTITLE 1,Installing Packages
# MAGIC %pip install {remote_wheel} -qqq
# MAGIC %pip install xlsxwriter -qqq
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Libraries Import and Setting UCX
import os
import logging
import threading
import shutil
from pathlib import Path
from threading import Lock
from functools import partial

import pandas as pd
import xlsxwriter

from databricks.sdk.config import with_user_agent_extra
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.dashboards import Dashboards
from databricks.labs.lsql.lakeview.model import Dataset
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext

# ctx
install_logger()
with_user_agent_extra("cmd", "export-assessment")
named_parameters = dict(config="/Workspace{config_file}")
ctx = RuntimeContext(named_parameters)
lock = Lock()

# COMMAND ----------

# DBTITLE 1,Assessment Export
FILE_NAME = "ucx_assessment_main.xlsx"
UCX_PATH = Path(f"/Workspace{{ctx.installation.install_folder()}}")
DOWNLOAD_PATH = Path("/dbfs/FileStore/excel-export/")


def _cleanup() -> None:
    '''Move the temporary results file to the download path and clean up the temp directory.'''
    shutil.move(
        UCX_PATH / "tmp" / FILE_NAME,
        DOWNLOAD_PATH / FILE_NAME,
    )
    shutil.rmtree(UCX_PATH / "tmp/")


def _prepare_directories() -> None:
    '''Ensure that the necessary directories exist.'''
    os.makedirs(UCX_PATH / "tmp/", exist_ok=True)
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

def _process_id_columns(df):
    id_columns = [col for col in df.columns if 'id' in col.lower()]

    if id_columns:
        for col in id_columns:
            df[col] = "'" + df[col].astype(str)
    return df

def _to_excel(dataset: Dataset, writer: ...) -> None:
    '''Execute a SQL query and write the result to an Excel sheet.'''
    worksheet_name = dataset.display_name[:31]
    df = spark.sql(dataset.query).toPandas()
    df = _process_id_columns(df)
    with lock:
        df.to_excel(writer, sheet_name=worksheet_name, index=False)


def _render_export() -> None:
    '''Render an HTML link for downloading the results.'''
    html_content = '''
    <style>@font-face{{font-family:'DM Sans';src:url(https://cdn.bfldr.com/9AYANS2F/at/p9qfs3vgsvnp5c7txz583vgs/dm-sans-regular.ttf?auto=webp&format=ttf) format('truetype');font-weight:400;font-style:normal}}body{{font-family:'DM Sans',Arial,sans-serif}}.export-container{{text-align:center;margin-top:20px}}.export-container h2{{color:#1B3139;font-size:24px;margin-bottom:20px}}.export-container a{{display:inline-block;padding:12px 25px;background-color:#1B3139;color:#fff;text-decoration:none;border-radius:4px;font-size:18px;font-weight:500;transition:background-color 0.3s ease,transform:translateY(-2px) ease}}.export-container a:hover{{background-color:#FF3621;transform:translateY(-2px)}}</style>
    <div class="export-container"><h2>Export Results</h2><a href='{workspace_host}/files/excel-export/ucx_assessment_main.xlsx?o={workspace_id}' target='_blank' download>Download Results</a></div>

    '''
    displayHTML(html_content)


def export_results() -> None:
    '''Main method to export results to an Excel file.'''
    _prepare_directories()

    assessment_dashboard = next(UCX_PATH.glob("dashboards/*Assessment (Main)*"))
    dashboard_datasets = Dashboards(ctx.workspace_client).get_dashboard(assessment_dashboard).datasets

    try:
        target = UCX_PATH / "tmp/ucx_assessment_main.xlsx"
        with pd.ExcelWriter(target, engine="xlsxwriter") as writer:
            tasks = []
            for dataset in dashboard_datasets:
                tasks.append(partial(_to_excel, dataset, writer))
                Threads.strict("exporting", tasks)
        _cleanup()
        _render_export()
    except Exception as e:
        print(f"Error exporting results ", e)

# COMMAND ----------

# DBTITLE 1,Data Export
export_results()
"""


class DeployedWorkflows:
    def __init__(self, ws: WorkspaceClient, install_state: InstallState):
        self._ws = ws
        self._install_state = install_state

    def run_workflow(self, step: str, skip_job_wait: bool = False, max_wait: timedelta = timedelta(minutes=20)) -> int:
        # this dunder variable is hiding this method from tracebacks, making it cleaner
        # for the user to see the actual error without too much noise.
        __tracebackhide__ = True  # pylint: disable=unused-variable
        job_id = int(self._install_state.jobs[step])
        logger.debug(f"starting {step} job: {self._ws.config.host}#job/{job_id}")
        job_initial_run = self._ws.jobs.run_now(job_id)
        run_id = job_initial_run.run_id
        if not run_id:
            raise NotFound(f"job run not found for {step}")
        run_url = f"{self._ws.config.host}#job/{job_id}/runs/{run_id}"
        logger.info(f"Started {step} job: {run_url}")
        if skip_job_wait:
            return run_id
        try:
            logger.debug(f"Waiting for completion of {step} job: {run_url}")
            job_run = self._ws.jobs.wait_get_run_job_terminated_or_skipped(run_id=run_id, timeout=max_wait)
            self._log_completed_job(step, run_id, job_run)
            return run_id
        except TimeoutError:
            logger.warning(f"Timeout while waiting for {step} job to complete: {run_url}")
            logger.info('---------- REMOTE LOGS --------------')
            self._relay_logs(step, run_id)
            logger.info('------ END REMOTE LOGS (SO FAR) -----')
            raise
        except OperationFailed as err:
            logger.info('---------- REMOTE LOGS --------------')
            self._relay_logs(step, run_id)
            logger.info('---------- END REMOTE LOGS ----------')
            job_run = self._ws.jobs.get_run(run_id)
            raise self._infer_error_from_job_run(job_run) from err

    @staticmethod
    def _log_completed_job(step: str, run_id: int, job_run: Run) -> None:
        if job_run.state:
            result_state = job_run.state.result_state or "N/A"
            state_message = job_run.state.state_message
            state_description = f"{result_state} ({state_message})" if state_message else f"{result_state}"
            logger.info(f"Completed {step} job run {run_id} with state: {state_description}")
        else:
            logger.warning(f"Completed {step} job run {run_id} but end state is unknown.")
        if job_run.start_time or job_run.end_time:
            start_time = (
                datetime.fromtimestamp(job_run.start_time / 1000, tz=timezone.utc) if job_run.start_time else None
            )
            end_time = datetime.fromtimestamp(job_run.end_time / 1000, tz=timezone.utc) if job_run.end_time else None
            if job_run.run_duration:
                duration = timedelta(milliseconds=job_run.run_duration)
            elif start_time and end_time:
                duration = end_time - start_time
            else:
                duration = None
            logger.info(
                f"Completed {step} job run {run_id} duration: {duration or 'N/A'} ({start_time or 'N/A'} thru {end_time or 'N/A'})"
            )

    def repair_run(self, workflow, verify_timeout: timedelta = timedelta(minutes=2)):
        try:
            job_id, run_id = self._repair_workflow(workflow, verify_timeout)
            run_details = self._ws.jobs.get_run(run_id=run_id, include_history=True)
            self._handle_repair_run(run_details, job_id, run_id, workflow)
        except InvalidParameterValue as e:
            logger.warning(f"Skipping {workflow}: {e}")
        except TimeoutError:
            logger.warning(f"Skipping the {workflow} due to time out. Please try after sometime")

    def _handle_repair_run(self, run_details, job_id, run_id, workflow):
        if run_details.repair_history:
            latest_repair_run_id = run_details.repair_history[-1].id
            job_url = f"{self._ws.config.host}#job/{job_id}/run/{run_id}"
            logger.debug(f"Repairing {workflow} job: {job_url}")
            self._ws.jobs.repair_run(run_id=run_id, rerun_all_failed_tasks=True, latest_repair_id=latest_repair_run_id)
            webbrowser.open(job_url)
        else:
            logger.warning(f"No repair history found for run_id={run_id}")

    def latest_job_status(self) -> list[dict]:
        latest_status = []
        for workflow_name, job_id in self._install_state.jobs.items():
            job_state = None
            start_time = None
            try:
                job_runs = list(self._ws.jobs.list_runs(job_id=int(job_id), limit=1))
            except InvalidParameterValue as e:
                logger.warning(f"skipping {workflow_name}: {e}")
                continue
            if job_runs:
                state = job_runs[0].state
                if state and state.result_state:
                    job_state = state.result_state.name
                elif state and state.life_cycle_state:
                    job_state = state.life_cycle_state.name
                if job_runs[0].start_time:
                    start_time = job_runs[0].start_time / 1000
            latest_status.append(
                {
                    # "Step" was the historic name that UCX used for the workflows, to "step" through a migration.
                    "step": workflow_name,
                    "state": "UNKNOWN" if not (job_runs and job_state) else job_state,
                    "started": (
                        "<never run>" if not (job_runs and start_time) else self._readable_timedelta(start_time)
                    ),
                }
            )
        return latest_status

    def validate_step(self, step: str, *, timeout: dt.timedelta = dt.timedelta(minutes=20)) -> bool:
        """Validate a workflow has completed successfully.

        If none of the job runs belonging to the workflow did not finish successfully (yet) and at least one job run is
        running or pending, we wait for the given timeout for that job run to finish. Thereafter, if none of the running
        or pending job runs, finished within the timeout, we consider the step to be invalid, i.e. we return `False`.

        Args :
            step (str) : The workflow name; step in the migration process.
            timeout (datetime.timedelta, optional) : The timeout to wait for a running or pending job to finish.
                Defaults to 20 minutes.

        Returns :
            bool : True if step is validate. False otherwise.
        """
        job_id = int(self._install_state.jobs[step])
        logger.debug(f"Validating {step} workflow: {self._ws.config.host}#job/{job_id}")
        current_runs = list(self._ws.jobs.list_runs(completed_only=False, job_id=job_id))
        for run in current_runs:
            if run.state and run.state.result_state == RunResultState.SUCCESS:
                return True
        for run in current_runs:
            if (
                run.run_id
                and run.state
                and run.state.life_cycle_state in (RunLifeCycleState.RUNNING, RunLifeCycleState.PENDING)
            ):
                logger.info("Identified a run in progress waiting for run completion")
                try:
                    self._ws.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id, timeout=timeout)
                except TimeoutError:
                    return False
                run_new_state = self._ws.jobs.get_run(run_id=run.run_id).state
                return run_new_state is not None and run_new_state.result_state == RunResultState.SUCCESS
        return False

    def relay_logs(self, workflow: str | None = None):
        latest_run = None
        if not workflow:
            runs = []
            for step in self._install_state.jobs:
                try:
                    _, latest_run = self._latest_job_run(step)
                    runs.append((step, latest_run))
                except InvalidParameterValue:
                    continue
            if not runs:
                logger.warning("No jobs to relay logs for")
                return
            runs = sorted(runs, key=lambda x: x[1].start_time, reverse=True)
            workflow, latest_run = runs[0]
        if not latest_run:
            assert workflow is not None
            _, latest_run = self._latest_job_run(workflow)
        self._relay_logs(workflow, latest_run.run_id)

    def _relay_logs(self, workflow, run_id):
        for record in self._fetch_last_run_attempt_logs(workflow, run_id):
            task_logger = logging.getLogger(record.component)
            MaxedStreamHandler.install_handler(task_logger)
            task_logger.setLevel(logger.getEffectiveLevel())
            log_level = logging.getLevelName(record.level)
            task_logger.log(log_level, record.message)
        MaxedStreamHandler.uninstall_handlers()

    def _fetch_last_run_attempt_logs(self, workflow: str, run_id: str) -> Iterator[PartialLogRecord]:
        """Fetch the logs for the last run attempt."""
        run_folders = self._get_log_run_folders(workflow, run_id)
        if not run_folders:
            return
        # sort folders based on the last repair attempt
        last_attempt = sorted(run_folders, key=lambda _: int(_.split('-')[-1]), reverse=True)[0]
        for object_info in self._ws.workspace.list(last_attempt):
            if not object_info.path:
                continue
            if '.log' not in object_info.path:
                continue
            task_name = os.path.basename(object_info.path).split('.')[0]
            with self._ws.workspace.download(object_info.path) as raw_file:
                text_io = StringIO(raw_file.read().decode())
            for record in parse_logs(text_io):
                yield replace(record, component=f'{record.component}:{task_name}')

    def _get_log_run_folders(self, workflow: str, run_id: str) -> list[str]:
        """Get the log run folders.

        The log run folders are located in the installation folder under the logs directory. Each workflow has a log run
        folder for each run id. Multiple runs occur for repair runs.
        """
        log_path = f"{self._install_state.install_folder()}/logs/{workflow}"
        try:
            # Ensure any exception is triggered early.
            log_path_objects = list(self._ws.workspace.list(log_path))
        except ResourceDoesNotExist:
            logger.warning(f"Cannot fetch logs as folder {log_path} does not exist")
            return []
        run_folders = []
        for run_folder in log_path_objects:
            if not run_folder.path or run_folder.object_type != ObjectType.DIRECTORY:
                continue
            if f"run-{run_id}-" not in run_folder.path:
                continue
            run_folders.append(run_folder.path)
        return run_folders

    @staticmethod
    def _readable_timedelta(epoch):
        when = datetime.utcfromtimestamp(epoch)
        duration = datetime.now() - when
        data = {}
        data["days"], remaining = divmod(duration.total_seconds(), 86_400)
        data["hours"], remaining = divmod(remaining, 3_600)
        data["minutes"], data["seconds"] = divmod(remaining, 60)

        time_parts = ((name, round(value)) for (name, value) in data.items())
        time_parts = [f"{value} {name[:-1] if value == 1 else name}" for name, value in time_parts if value > 0]
        if len(time_parts) > 0:
            time_parts.append("ago")
        if time_parts:
            return " ".join(time_parts)
        return "less than 1 second ago"

    def _repair_workflow(self, workflow, verify_timeout):
        job_id, latest_job_run = self._latest_job_run(workflow)
        retry_on_attribute_error = retried(on=[AttributeError], timeout=verify_timeout)
        retried_check = retry_on_attribute_error(self._get_result_state)
        state_value = retried_check(job_id)
        logger.info(f"The status for the latest run is {state_value}")
        if state_value != "FAILED":
            raise InvalidParameterValue("job is not in FAILED state hence skipping repair")
        run_id = latest_job_run.run_id
        return job_id, run_id

    def _latest_job_run(self, workflow: str):
        job_id = self._install_state.jobs.get(workflow)
        if not job_id:
            raise InvalidParameterValue("job does not exists hence skipping repair")
        job_runs = list(self._ws.jobs.list_runs(job_id=job_id, limit=1))
        if not job_runs:
            raise InvalidParameterValue("job is not initialized yet. Can't trigger repair run now")
        latest_job_run = job_runs[0]
        return job_id, latest_job_run

    def _get_result_state(self, job_id):
        job_runs = list(self._ws.jobs.list_runs(job_id=job_id, limit=1))
        latest_job_run = job_runs[0]
        if not latest_job_run.state.result_state:
            raise AttributeError("no result state in job run")
        job_state = latest_job_run.state.result_state.value
        return job_state

    def _infer_error_from_job_run(self, job_run) -> Exception:
        errors: list[Exception] = []
        timeouts: list[DeadlineExceeded] = []
        assert job_run.tasks is not None
        for run_task in job_run.tasks:
            error = self._infer_error_from_task_run(run_task)
            if not error:
                continue
            if isinstance(error, DeadlineExceeded):
                timeouts.append(error)
                continue
            errors.append(error)
        assert job_run.state is not None
        assert job_run.state.state_message is not None
        if len(errors) == 1:
            return errors[0]
        all_errors = errors + timeouts
        if len(all_errors) == 0:
            return Unknown(job_run.state.state_message)
        return ManyError(all_errors)

    def _infer_error_from_task_run(self, run_task: jobs.RunTask) -> Exception | None:
        if not run_task.state:
            return None
        if run_task.state.result_state == jobs.RunResultState.TIMEDOUT:
            msg = f"{run_task.task_key}: The run was stopped after reaching the timeout"
            return DeadlineExceeded(msg)
        if run_task.state.result_state != jobs.RunResultState.FAILED:
            return None
        assert run_task.run_id is not None
        run_output = self._ws.jobs.get_run_output(run_task.run_id)
        if not run_output:
            msg = f'No run output. {run_task.state.state_message}'
            return InternalError(msg)
        if logger.isEnabledFor(logging.DEBUG):
            if run_output.error_trace:
                sys.stderr.write(run_output.error_trace)
        if not run_output.error:
            msg = f'No error in run output. {run_task.state.state_message}'
            return InternalError(msg)
        return self._infer_task_exception(f"{run_task.task_key}: {run_output.error}")

    @staticmethod
    def _infer_task_exception(haystack: str) -> Exception:
        needles: list[type[Exception]] = [
            BadRequest,
            Unauthenticated,
            PermissionDenied,
            NotFound,
            ResourceConflict,
            TooManyRequests,
            Cancelled,
            databricks.sdk.errors.NotImplemented,
            InternalError,
            TemporarilyUnavailable,
            DeadlineExceeded,
            InvalidParameterValue,
            ResourceDoesNotExist,
            Aborted,
            AlreadyExists,
            ResourceAlreadyExists,
            ResourceExhausted,
            RequestLimitExceeded,
            Unknown,
            DataLoss,
            ValueError,
            KeyError,
        ]
        constructors: dict[re.Pattern, type[Exception]] = {
            re.compile(r".*\[TABLE_OR_VIEW_NOT_FOUND] (.*)"): NotFound,
            re.compile(r".*\[SCHEMA_NOT_FOUND] (.*)"): NotFound,
            re.compile(r".*\[TimeoutException] (.*)"): TimeoutError,
        }
        for klass in needles:
            constructors[re.compile(f".*{klass.__name__}: (.*)")] = klass
        for pattern, klass in constructors.items():
            match = pattern.match(haystack)
            if match:
                return klass(match.group(1))
        return Unknown(haystack)


class WorkflowsDeployment(InstallationMixin):
    def __init__(
        self,
        config: WorkspaceConfig,
        installation: Installation,
        install_state: InstallState,
        ws: WorkspaceClient,
        wheels: WheelsV2,
        product_info: ProductInfo,
        workflows: Workflows,
    ):
        self._config = config
        self._installation = installation
        self._ws = ws
        self._install_state = install_state
        self._wheels = wheels
        self._product_info = product_info
        self._workflows = workflows.workflows
        self._this_file = Path(__file__)
        super().__init__(config, installation, ws)

    def create_jobs(self) -> None:
        remote_wheels = self._upload_wheel()
        desired_workflows = {
            workflow
            for workflow, tasks in self._workflows.items()
            if any(task.cloud_compatible(self._ws.config) for task in tasks.tasks())
        }

        wheel_runner = ""
        if self._config.override_clusters:
            wheel_runner = self._upload_wheel_runner(remote_wheels)
        for workflow_name in desired_workflows:
            settings = self._job_settings(workflow_name, remote_wheels)
            if self._config.override_clusters:
                settings = self._apply_cluster_overrides(
                    workflow_name,
                    settings,
                    self._config.override_clusters,
                    wheel_runner,
                )
            self._deploy_workflow(workflow_name, settings)

        self.remove_jobs(keep=desired_workflows)
        self._install_state.save()
        self._create_debug(remote_wheels)
        self._create_export(remote_wheels)
        self._create_readme()

    def remove_jobs(self, *, keep: set[str] | None = None) -> None:
        for workflow_name, job_id in self._install_state.jobs.items():
            if keep and workflow_name in keep:
                continue
            try:
                if not self._is_managed_job_failsafe(int(job_id)):
                    logger.warning(f"Corrupt installation state. Skipping job_id={job_id} as it is not managed by UCX")
                    continue
                logger.info(f"Removing job_id={job_id}, as it is no longer needed")
                self._ws.jobs.delete(job_id)
            except InvalidParameterValue:
                logger.warning(f"step={workflow_name} does not exist anymore for some reason")
                continue

    # see https://github.com/databrickslabs/ucx/issues/2667
    def _is_managed_job_failsafe(self, job_id: int) -> bool:
        install_folder = self._installation.install_folder()
        try:
            return self._is_managed_job(job_id, install_folder)
        except ResourceDoesNotExist:
            return False
        except InvalidParameterValue:
            return False

    def _is_managed_job(self, job_id: int, install_folder: str) -> bool:
        job = self._ws.jobs.get(job_id)
        if not job.settings or not job.settings.tasks:
            return False
        for task in job.settings.tasks:
            if task.notebook_task and task.notebook_task.notebook_path.startswith(install_folder):
                return True
            if task.python_wheel_task and task.python_wheel_task.package_name == "databricks_labs_ucx":
                return True
        return False

    @property
    def _config_file(self):
        return f"{self._installation.install_folder()}/config.yml"

    def _is_testing(self):
        return self._product_info.product_name() != "ucx"

    @staticmethod
    def _is_nightly():
        ci_env = os.getenv("TEST_NIGHTLY")
        return ci_env is not None and ci_env.lower() == "true"

    @classmethod
    def _get_test_purge_time(cls) -> str:
        # Duplicate of mixins.fixtures.get_test_purge_time(); we don't want to import pytest as a transitive dependency.
        timeout = TEST_NIGHTLY_CI_RESOURCES_PURGE_TIMEOUT if cls._is_nightly() else TEST_RESOURCE_PURGE_TIMEOUT
        now = datetime.now(timezone.utc)
        purge_deadline = now + timeout
        # Round UP to the next hour boundary: that is when resources will be deleted.
        purge_hour = purge_deadline + (datetime.min.replace(tzinfo=timezone.utc) - purge_deadline) % timedelta(hours=1)
        return purge_hour.strftime("%Y%m%d%H")

    def _create_readme(self) -> None:
        debug_notebook_link = self._installation.workspace_markdown_link('debug notebook', 'DEBUG.py')
        markdown = [
            "# UCX - The Unity Catalog Migration Assistant",
            f'To troubleshoot, see {debug_notebook_link}.\n',
            "Here are the URLs and descriptions of workflows that trigger various stages of migration.",
            "All jobs are defined with necessary cluster configurations and DBR versions.\n",
        ]
        for workflow_name in self._workflow_names():
            if workflow_name not in self._install_state.jobs:
                logger.warning(f"Skipping step '{workflow_name}' since it was not deployed.")
                continue
            job_id = self._install_state.jobs[workflow_name]
            dashboard_link = ""
            dashboard_link = self._create_dashboard_links(workflow_name, dashboard_link)
            job_link = f"[{self._name(workflow_name)}]({self._ws.config.host}#job/{job_id})"
            markdown.append("---\n\n")
            markdown.append(f"## {job_link}\n\n")
            markdown.append(f"{dashboard_link}")
            markdown.append("\nThe workflow consists of the following separate tasks:\n\n")
            for task in self._workflows[workflow_name].tasks():
                if self._is_testing() and task.is_testing():
                    continue
                doc = self._config.replace_inventory_variable(task.doc)
                markdown.append(f"### `{task.name}`\n\n")
                markdown.append(f"{doc}\n")
                markdown.append("\n\n")
        preamble = ["# Databricks notebook source", "# MAGIC %md"]
        intro = "\n".join(preamble + [f"# MAGIC {line}" for line in markdown])
        self._installation.upload('README.py', intro.encode('utf8'))

    def _create_dashboard_links(self, workflow_name, dashboard_link):
        dashboards_per_step = [d for d in self._install_state.dashboards.keys() if d.startswith(workflow_name)]
        for dash in dashboards_per_step:
            if len(dashboard_link) == 0:
                dashboard_link += "Go to the one of the following dashboards after running the job:\n"
            first, second = dash.replace("_", " ").title().split()
            dashboard_url = Dashboards(self._ws).get_url(self._install_state.dashboards[dash])
            dashboard_link += f"  - [{first} ({second}) dashboard]({dashboard_url})\n"
        return dashboard_link

    def _workflow_names(self) -> list[str]:
        names = []
        # Workflows are excluded if _is_testing() and all tasks are testing.
        for workflow_name, tasks in self._workflows.items():
            if self._is_testing() and all(t.is_testing() for t in tasks.tasks()):
                continue
            names.append(workflow_name)
        return names

    def _job_cluster_spark_conf(self, cluster_key: str):
        conf_from_installation = self._config.spark_conf if self._config.spark_conf else {}
        if cluster_key == "main":
            spark_conf = {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]",
            }
            return spark_conf | conf_from_installation
        if cluster_key == "tacl":
            return {"spark.databricks.acl.sqlOnly": "true"} | conf_from_installation
        if cluster_key == "user_isolation":
            return {"spark.sql.sources.parallelPartitionDiscovery.parallelism": "200"} | conf_from_installation
        return conf_from_installation

    # Workflow creation might fail on an InternalError with no message
    @retried(on=[InternalError], timeout=timedelta(minutes=2))
    def _deploy_workflow(self, workflow_name: str, settings):
        if workflow_name in self._install_state.jobs:
            try:
                job_id = int(self._install_state.jobs[workflow_name])
                logger.info(f"Updating configuration for step={workflow_name} job_id={job_id}")
                return self._ws.jobs.reset(job_id, jobs.JobSettings(**settings))
            except InvalidParameterValue:
                del self._install_state.jobs[workflow_name]
                logger.warning(f"step={workflow_name} does not exist anymore for some reason")
                return self._deploy_workflow(workflow_name, settings)
        logger.info(f"Creating new job configuration for step={workflow_name}")
        new_job = self._ws.jobs.create(**settings)
        assert new_job.job_id is not None
        self._install_state.jobs[workflow_name] = str(new_job.job_id)
        return None

    @staticmethod
    def _library_dep_order(library: str):
        match library:
            case library if 'sdk' in library:
                return 0
            case library if 'blueprint' in library:
                return 1
            case library if 'sqlglot' in library:
                return 2
            case _:
                return 3

    def _upload_wheel(self):
        wheel_paths = []
        with self._wheels:
            if self._config.upload_dependencies:
                wheel_paths = self._wheels.upload_wheel_dependencies(["databricks", "sqlglot", "astroid"])
            wheel_paths.sort(key=WorkflowsDeployment._library_dep_order)
            wheel_paths.append(self._wheels.upload_to_wsfs())
            wheel_paths = [f"/Workspace{wheel}" for wheel in wheel_paths]
            return wheel_paths

    def _upload_wheel_runner(self, remote_wheels: list[str]) -> str:
        # TODO: we have to be doing this workaround until ES-897453 is solved in the platform
        remote_wheels_str = " ".join(remote_wheels)
        code = TEST_RUNNER_NOTEBOOK.format(remote_wheel=remote_wheels_str, config_file=self._config_file).encode("utf8")
        return self._installation.upload(f"wheels/wheel-test-runner-{self._product_info.version()}.py", code)

    @staticmethod
    def _apply_cluster_overrides(
        workflow_name: str,
        settings: dict[str, Any],
        overrides: dict[str, str],
        wheel_runner: str,
    ) -> dict:
        settings["job_clusters"] = [_ for _ in settings["job_clusters"] if _.job_cluster_key not in overrides]
        for job_task in settings["tasks"]:
            if job_task.job_cluster_key is None:
                continue
            if job_task.job_cluster_key in overrides:
                job_task.existing_cluster_id = overrides[job_task.job_cluster_key]
                job_task.job_cluster_key = None
                job_task.libraries = None
            if job_task.python_wheel_task is not None:
                job_task.python_wheel_task = None
                widget_values = {"task": job_task.task_key, 'workflow': workflow_name} | EXTRA_TASK_PARAMS
                job_task.notebook_task = jobs.NotebookTask(notebook_path=wheel_runner, base_parameters=widget_values)
        return settings

    def _job_settings(self, workflow_name: str, remote_wheels: list[str]) -> dict[str, Any]:
        email_notifications = None
        if not self._config.override_clusters and "@" in self._my_username:
            # set email notifications only if we're running the real
            # installation and not the integration test.
            email_notifications = jobs.JobEmailNotifications(
                on_success=[self._my_username], on_failure=[self._my_username]
            )

        job_tasks = []
        job_clusters: set[str] = {Task.job_cluster}
        workflow = self._workflows[workflow_name]
        for task in workflow.tasks():
            job_clusters.add(task.job_cluster)
            job_tasks.append(self._job_task(task, remote_wheels))
        job_tasks.append(self._job_parse_logs_task(job_tasks, workflow_name, remote_wheels))
        version = self._product_info.version()
        version = version if not self._ws.config.is_gcp else version.replace("+", "-")
        tags = {"version": f"v{version}"}
        if self._is_testing():
            # add RemoveAfter tag for test job cleanup
            date_to_remove = self._get_test_purge_time()
            tags.update({"RemoveAfter": date_to_remove})
        return {
            "name": self._name(workflow_name),
            "tags": tags,
            "job_clusters": self._job_clusters(job_clusters),
            "email_notifications": email_notifications,
            "schedule": workflow.schedule,
            "tasks": job_tasks,
        }

    def _job_task(self, task: Task, remote_wheels: list[str]) -> jobs.Task:
        jobs_task = jobs.Task(
            task_key=task.name,
            job_cluster_key=task.job_cluster,
            depends_on=[jobs.TaskDependency(task_key=d) for d in task.dependencies()],
        )
        if task.notebook:
            return self._job_notebook_task(jobs_task, task)
        return self._job_wheel_task(jobs_task, task.workflow, remote_wheels)

    def _job_notebook_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        assert task.notebook is not None
        local_notebook = self._this_file.parent.parent / task.notebook
        with local_notebook.open("rb") as f:
            remote_notebook = self._installation.upload(local_notebook.name, f.read())
        return replace(
            jobs_task,
            notebook_task=jobs.NotebookTask(
                notebook_path=remote_notebook,
                # ES-872211: currently, we cannot read WSFS files from Scala context
                base_parameters={
                    "task": task.name,
                    "config": f"/Workspace{self._config_file}",
                }
                | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_wheel_task(self, jobs_task: jobs.Task, workflow: str, remote_wheels: list[str]) -> jobs.Task:
        libraries = []
        for wheel in remote_wheels:
            libraries.append(compute.Library(whl=wheel))
        named_parameters = {
            "config": f"/Workspace{self._config_file}",
            "workflow": workflow,
            "task": jobs_task.task_key,
        }
        return replace(
            jobs_task,
            libraries=libraries,
            python_wheel_task=jobs.PythonWheelTask(
                package_name="databricks_labs_ucx",
                entry_point="runtime",  # [project.entry-points.databricks] in pyproject.toml
                named_parameters=named_parameters | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_clusters(self, names: set[str]):
        clusters = []
        if "main" in names:
            clusters.append(
                jobs.JobCluster(
                    job_cluster_key="main",
                    new_cluster=compute.ClusterSpec(
                        data_security_mode=compute.DataSecurityMode.LEGACY_SINGLE_USER_STANDARD,
                        spark_conf=self._job_cluster_spark_conf("main"),
                        custom_tags={"ResourceClass": "SingleNode"},
                        num_workers=0,
                        policy_id=self._config.policy_id,
                        apply_policy_default_values=True,
                    ),
                )
            )
        if "tacl" in names:
            clusters.append(
                jobs.JobCluster(
                    job_cluster_key="tacl",
                    new_cluster=compute.ClusterSpec(
                        data_security_mode=compute.DataSecurityMode.LEGACY_TABLE_ACL,
                        spark_conf=self._job_cluster_spark_conf("tacl"),
                        num_workers=1,  # ShowPermissionsCommand needs a worker
                        policy_id=self._config.policy_id,
                        apply_policy_default_values=True,
                    ),
                )
            )
        if "user_isolation" in names:
            clusters.append(
                jobs.JobCluster(
                    job_cluster_key="user_isolation",
                    new_cluster=compute.ClusterSpec(
                        data_security_mode=compute.DataSecurityMode.USER_ISOLATION,
                        spark_conf=self._job_cluster_spark_conf("user_isolation"),
                        policy_id=self._config.policy_id,
                        autoscale=compute.AutoScale(
                            max_workers=self._config.max_workers,
                            min_workers=self._config.min_workers,
                        ),
                        apply_policy_default_values=True,
                    ),
                )
            )
        return clusters

    def _job_parse_logs_task(self, job_tasks: list[jobs.Task], workflow: str, remote_wheels: list[str]) -> jobs.Task:
        jobs_task = jobs.Task(
            task_key="parse_logs",
            job_cluster_key=Task.job_cluster,
            # The task dependents on all previous tasks.
            depends_on=[jobs.TaskDependency(task_key=task.task_key) for task in job_tasks],
            run_if=jobs.RunIf.ALL_DONE,
        )
        return self._job_wheel_task(jobs_task, workflow, remote_wheels)

    def _create_debug(self, remote_wheels: list[str]):
        readme_link = self._installation.workspace_link('README')
        job_links = ", ".join(
            f"[{self._name(workflow_name)}]({self._ws.config.host}#job/{job_id})"
            for workflow_name, job_id in self._install_state.jobs.items()
        )
        remote_wheels_str = " ".join(remote_wheels)
        content = DEBUG_NOTEBOOK.format(
            remote_wheel=remote_wheels_str, readme_link=readme_link, job_links=job_links, config_file=self._config_file
        ).encode("utf8")
        self._installation.upload('DEBUG.py', content)

    def _create_export(self, remote_wheels: list[str]):
        remote_wheels_str = " ".join(remote_wheels)
        content = EXPORT_TO_EXCEL_NOTEBOOK.format(
            remote_wheel=remote_wheels_str,
            config_file=self._config_file,
            workspace_host=self._ws.config.host,
            workspace_id=self._ws.get_workspace_id(),
        ).encode("utf8")
        self._installation.upload('EXPORT_ASSESSMENT_TO_EXCEL.py', content)


class MaxedStreamHandler(logging.StreamHandler):

    MAX_STREAM_SIZE = 2**20 - 2**6  # 1 Mb minus some buffer
    _installed_handlers: dict[str, tuple[logging.Logger, MaxedStreamHandler]] = {}
    _sent_bytes = 0

    @classmethod
    def install_handler(cls, logger_: logging.Logger):
        if logger_.handlers:
            # already installed ?
            installed = next((h for h in logger_.handlers if isinstance(h, MaxedStreamHandler)), None)
            if installed:
                return
            # any handler to override ?
            handler = next((h for h in logger_.handlers if isinstance(h, logging.StreamHandler)), None)
            if handler:
                to_install = MaxedStreamHandler(handler)
                cls._installed_handlers[logger_.name] = (logger_, to_install)
                logger_.removeHandler(handler)
                logger_.addHandler(to_install)
                return
        if logger_.parent:
            cls.install_handler(logger_.parent)
        if logger_.root:
            cls.install_handler(logger_.root)

    @classmethod
    def uninstall_handlers(cls):
        for logger_, handler in cls._installed_handlers.values():
            logger_.removeHandler(handler)
            logger_.addHandler(handler.original_handler)
        cls._installed_handlers.clear()
        cls._sent_bytes = 0

    def __init__(self, original_handler: logging.StreamHandler):
        super().__init__()
        self._original_handler = original_handler

    @property
    def original_handler(self):
        return self._original_handler

    def emit(self, record):
        try:
            msg = self.format(record) + self.terminator
            if self._prevent_overflow(msg):
                return
            self.stream.write(msg)
            self.flush()
        except RecursionError:  # See issue 36272
            raise
        # the below is copied from Python source
        # so ensuring not to break the logging logic
        # pylint: disable=broad-exception-caught
        except Exception:
            self.handleError(record)

    def _prevent_overflow(self, msg: str):
        data = msg.encode("utf-8")
        if self._sent_bytes + len(data) > self.MAX_STREAM_SIZE:
            # ensure readers are aware of why the logs are incomplete
            self.stream.write(f"MAX LOGS SIZE REACHED: {self._sent_bytes} bytes!!!")
            self.flush()
            return True
        return False

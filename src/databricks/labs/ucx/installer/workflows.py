import functools
import logging
import os
import re
import sys
import webbrowser
from dataclasses import replace
from datetime import timedelta
from typing import Any

import databricks.sdk.errors
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.installer import InstallState
from databricks.sdk.retries import retried

from databricks.labs.ucx.framework.tasks import _TASKS, Task
from databricks.labs.blueprint.parallel import ManyError, Threads, Task
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.upgrades import Upgrades
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2, find_project_root
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed, BadRequest, Unauthenticated, PermissionDenied, NotFound, \
    ResourceConflict, TooManyRequests, Cancelled, InternalError, TemporarilyUnavailable, DeadlineExceeded, \
    InvalidParameterValue, ResourceDoesNotExist, Aborted, AlreadyExists, ResourceAlreadyExists, ResourceExhausted, \
    RequestLimitExceeded, Unknown, DataLoss
from databricks.sdk.service import jobs, compute

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.configure import ConfigureClusterOverrides
from databricks.labs.ucx.install import InstallationMixin
from databricks.labs.ucx.runtime import main

logger = logging.getLogger(__name__)

EXTRA_TASK_PARAMS = {
    "job_id": "{{job_id}}",
    "run_id": "{{run_id}}",
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

# MAGIC %pip install /Workspace{remote_wheel}
dbutils.library.restartPython()

# COMMAND ----------

import logging
from pathlib import Path
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.sdk import WorkspaceClient

install_logger()
logging.getLogger("databricks").setLevel("DEBUG")

cfg = Installation.load_local(WorkspaceConfig, Path("/Workspace{config_file}"))
ws = WorkspaceClient()

print(__version__)
"""

TEST_RUNNER_NOTEBOOK = """# Databricks notebook source
# MAGIC %pip install /Workspace{remote_wheel}
dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.ucx.runtime import main

main(f'--config=/Workspace{config_file}',
     f'--task=' + dbutils.widgets.get('task'),
     f'--job_id=' + dbutils.widgets.get('job_id'),
     f'--run_id=' + dbutils.widgets.get('run_id'),
     f'--parent_run_id=' + dbutils.widgets.get('parent_run_id'))
"""


class WorkflowsInstallation(InstallationMixin):
    def __init__(self,
                 config: WorkspaceConfig,
                 installation: Installation,
                 ws: WorkspaceClient,
                 wheels: WheelsV2,
                 prompts: Prompts,
                 product_info: ProductInfo,
                 verify_timeout: timedelta):
        self._config = config
        self._installation = installation
        self._ws = ws
        self._state = InstallState.from_installation(installation)
        self._wheels = wheels
        self._prompts = prompts
        self._product_info = product_info
        self._verify_timeout = verify_timeout
        super().__init__(config, installation, ws)

    def run_workflow(self, step: str):
        job_id = int(self._state.jobs[step])
        logger.debug(f"starting {step} job: {self._ws.config.host}#job/{job_id}")
        job_run_waiter = self._ws.jobs.run_now(job_id)
        try:
            job_run_waiter.result()
        except OperationFailed as err:
            # currently we don't have any good message from API, so we have to work around it.
            job_run = self._ws.jobs.get_run(job_run_waiter.run_id)
            raise self._infer_error_from_job_run(job_run) from err

    def create_jobs(self):
        logger.debug(f"Creating jobs from tasks in {main.__name__}")
        remote_wheel = self._upload_wheel()
        desired_steps = {t.workflow for t in _TASKS.values() if t.cloud_compatible(self._ws.config)}
        wheel_runner = None

        if self._config.override_clusters:
            wheel_runner = self._upload_wheel_runner(remote_wheel)
        for step_name in desired_steps:
            settings = self._job_settings(step_name, remote_wheel)
            if self._config.override_clusters:
                settings = self._apply_cluster_overrides(settings, self._config.override_clusters, wheel_runner)
            self._deploy_workflow(step_name, settings)

        for step_name, job_id in self._state.jobs.items():
            if step_name not in desired_steps:
                try:
                    logger.info(f"Removing job_id={job_id}, as it is no longer needed")
                    self._ws.jobs.delete(job_id)
                except InvalidParameterValue:
                    logger.warning(f"step={step_name} does not exist anymore for some reason")
                    continue

        self._state.save()
        self._create_debug(remote_wheel)

    def repair_run(self, workflow):
        try:
            job_id, run_id = self._repair_workflow(workflow)
            run_details = self._ws.jobs.get_run(run_id=run_id, include_history=True)
            latest_repair_run_id = run_details.repair_history[-1].id
            job_url = f"{self._ws.config.host}#job/{job_id}/run/{run_id}"
            logger.debug(f"Repair Running {workflow} job: {job_url}")
            self._ws.jobs.repair_run(run_id=run_id, rerun_all_failed_tasks=True, latest_repair_id=latest_repair_run_id)
            webbrowser.open(job_url)
        except InvalidParameterValue as e:
            logger.warning(f"skipping {workflow}: {e}")
        except TimeoutError:
            logger.warning(f"Skipping the {workflow} due to time out. Please try after sometime")

    def latest_job_status(self) -> list[dict]:
        latest_status = []
        for step, job_id in self._state.jobs.items():
            job_state = None
            start_time = None
            try:
                job_runs = list(self._ws.jobs.list_runs(job_id=int(job_id), limit=1))
            except InvalidParameterValue as e:
                logger.warning(f"skipping {step}: {e}")
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
                    "step": step,
                    "state": "UNKNOWN" if not (job_runs and job_state) else job_state,
                    "started": (
                        "<never run>" if not (job_runs and start_time) else self._readable_timedelta(start_time)
                    ),
                }
            )
        return latest_status

    @property
    def _config_file(self):
        return f"{self._installation.install_folder()}/config.yml"

    def _deploy_workflow(self, step_name: str, settings):
        if step_name in self._state.jobs:
            try:
                job_id = int(self._state.jobs[step_name])
                logger.info(f"Updating configuration for step={step_name} job_id={job_id}")
                return self._ws.jobs.reset(job_id, jobs.JobSettings(**settings))
            except InvalidParameterValue:
                del self._state.jobs[step_name]
                logger.warning(f"step={step_name} does not exist anymore for some reason")
                return self._deploy_workflow(step_name, settings)
        logger.info(f"Creating new job configuration for step={step_name}")
        new_job = self._ws.jobs.create(**settings)
        assert new_job.job_id is not None
        self._state.jobs[step_name] = str(new_job.job_id)
        return None

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
        needles = [
            BadRequest,
            Unauthenticated,
            PermissionDenied,
            NotFound,
            ResourceConflict,
            TooManyRequests,
            Cancelled,
            InternalError,
            NotImplemented,
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
        }
        for klass in needles:
            constructors[re.compile(f".*{klass.__name__}: (.*)")] = klass
        for pattern, klass in constructors.items():
            match = pattern.match(haystack)
            if match:
                return klass(match.group(1))
        return Unknown(haystack)

    def _upload_wheel(self):
        with self._wheels:
            try:
                self._wheels.upload_to_dbfs()
            except PermissionDenied as err:
                if not self._prompts:
                    raise RuntimeWarning("no Prompts instance found") from err
                logger.warning(f"Uploading wheel file to DBFS failed, DBFS is probably write protected. {err}")
                configure_cluster_overrides = ConfigureClusterOverrides(self._ws, self._prompts.choice_from_dict)
                self._config.override_clusters = configure_cluster_overrides.configure()
                self._installation.save(self._config)
            return self._wheels.upload_to_wsfs()

    def _upload_wheel_runner(self, remote_wheel: str):
        # TODO: we have to be doing this workaround until ES-897453 is solved in the platform
        code = TEST_RUNNER_NOTEBOOK.format(remote_wheel=remote_wheel, config_file=self._config_file).encode("utf8")
        return self._installation.upload(f"wheels/wheel-test-runner-{self._product_info.version()}.py", code)

    @staticmethod
    def _apply_cluster_overrides(settings: dict[str, Any], overrides: dict[str, str], wheel_runner: str) -> dict:
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
                params = {"task": job_task.task_key} | EXTRA_TASK_PARAMS
                job_task.notebook_task = jobs.NotebookTask(notebook_path=wheel_runner, base_parameters=params)
        return settings

    def _job_settings(self, step_name: str, remote_wheel: str):
        email_notifications = None
        if not self._config.override_clusters and "@" in self._my_username:
            # set email notifications only if we're running the real
            # installation and not the integration test.
            email_notifications = jobs.JobEmailNotifications(
                on_success=[self._my_username], on_failure=[self._my_username]
            )
        tasks = sorted(
            [t for t in _TASKS.values() if t.workflow == step_name],
            key=lambda _: _.name,
        )
        version = self._product_info.version()
        version = version if not self._ws.config.is_gcp else version.replace("+", "-")
        return {
            "name": self._name(step_name),
            "tags": {"version": f"v{version}"},
            "job_clusters": self._job_clusters({t.job_cluster for t in tasks}),
            "email_notifications": email_notifications,
            "tasks": [self._job_task(task, remote_wheel) for task in tasks],
        }

    def _job_task(self, task: Task, remote_wheel: str) -> jobs.Task:
        jobs_task = jobs.Task(
            task_key=task.name,
            job_cluster_key=task.job_cluster,
            depends_on=[jobs.TaskDependency(task_key=d) for d in _TASKS[task.name].dependencies()],
        )
        if task.dashboard:
            # dashboards are created in parallel to wheel uploads, so we'll just retry
            retry_on_attribute_error = retried(on=[KeyError], timeout=self._verify_timeout)
            retried_job_dashboard_task = retry_on_attribute_error(self._job_dashboard_task)
            return retried_job_dashboard_task(jobs_task, task)
        if task.notebook:
            return self._job_notebook_task(jobs_task, task)
        return self._job_wheel_task(jobs_task, task, remote_wheel)

    def _job_dashboard_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        assert task.dashboard is not None
        dashboard_id = self._state.dashboards[task.dashboard]
        return replace(
            jobs_task,
            job_cluster_key=None,
            sql_task=jobs.SqlTask(
                warehouse_id=self._warehouse_id,
                dashboard=jobs.SqlTaskDashboard(dashboard_id=dashboard_id),
            ),
        )

    def _job_notebook_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        assert task.notebook is not None
        local_notebook = self._this_file.parent / task.notebook
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

    def _job_wheel_task(self, jobs_task: jobs.Task, task: Task, remote_wheel: str) -> jobs.Task:
        return replace(
            jobs_task,
            # TODO: check when we can install wheels from WSFS properly
            libraries=[compute.Library(whl=f"dbfs:{remote_wheel}")],
            python_wheel_task=jobs.PythonWheelTask(
                package_name="databricks_labs_ucx",
                entry_point="runtime",  # [project.entry-points.databricks] in pyproject.toml
                named_parameters={"task": task.name, "config": f"/Workspace{self._config_file}"} | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_clusters(self, names: set[str]):
        clusters = []
        spark_conf = {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        }
        if self._config.spark_conf is not None:
            spark_conf = spark_conf | self._config.spark_conf
        spec = compute.ClusterSpec(
            data_security_mode=compute.DataSecurityMode.LEGACY_SINGLE_USER,
            spark_conf=spark_conf,
            custom_tags={"ResourceClass": "SingleNode"},
            num_workers=0,
            policy_id=self._config.policy_id,
        )
        if "main" in names:
            clusters.append(
                jobs.JobCluster(
                    job_cluster_key="main",
                    new_cluster=spec,
                )
            )
        if "tacl" in names:
            clusters.append(
                jobs.JobCluster(
                    job_cluster_key="tacl",
                    new_cluster=replace(
                        spec,
                        data_security_mode=compute.DataSecurityMode.LEGACY_TABLE_ACL,
                        spark_conf={"spark.databricks.acl.sqlOnly": "true"},
                        num_workers=1,  # ShowPermissionsCommand needs a worker
                        custom_tags={},
                    ),
                )
            )
        return clusters

    def _create_debug(self, remote_wheel: str):
        readme_link = self._installation.workspace_link('README')
        job_links = ", ".join(
            f"[{self._name(step_name)}]({self._ws.config.host}#job/{job_id})"
            for step_name, job_id in self._state.jobs.items()
        )
        content = DEBUG_NOTEBOOK.format(
            remote_wheel=remote_wheel, readme_link=readme_link, job_links=job_links, config_file=self._config_file
        ).encode("utf8")
        self._installation.upload('DEBUG.py', content)

    def _repair_workflow(self, workflow):
        job_id = self._state.jobs.get(workflow)
        if not job_id:
            raise InvalidParameterValue("job does not exists hence skipping repair")
        job_runs = list(self._ws.jobs.list_runs(job_id=job_id, limit=1))
        if not job_runs:
            raise InvalidParameterValue("job is not initialized yet. Can't trigger repair run now")
        latest_job_run = job_runs[0]
        retry_on_attribute_error = retried(on=[AttributeError], timeout=self._verify_timeout)
        retried_check = retry_on_attribute_error(self._get_result_state)
        state_value = retried_check(job_id)
        logger.info(f"The status for the latest run is {state_value}")
        if state_value != "FAILED":
            raise InvalidParameterValue("job is not in FAILED state hence skipping repair")
        run_id = latest_job_run.run_id
        return job_id, run_id

    def _get_result_state(self, job_id):
        job_runs = list(self._ws.jobs.list_runs(job_id=job_id, limit=1))
        latest_job_run = job_runs[0]
        if not latest_job_run.state.result_state:
            raise AttributeError("no result state in job run")
        job_state = latest_job_run.state.result_state.value
        return job_state



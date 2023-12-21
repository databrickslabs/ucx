import functools
import json
import logging
import os
import sys
import time
import webbrowser
from dataclasses import replace
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    InvalidParameterValue,
    NotFound,
    OperationFailed,
    PermissionDenied,
)
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk.service.sql import EndpointInfoWarehouseType, SpotInstancePolicy
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.azure import AzureServicePrincipalInfo
from databricks.labs.ucx.assessment.clusters import ClusterInfo
from databricks.labs.ucx.assessment.init_scripts import GlobalInitScriptInfo
from databricks.labs.ucx.assessment.jobs import JobInfo
from databricks.labs.ucx.assessment.pipelines import PipelineInfo
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.configure import ConfigureClusterOverrides
from databricks.labs.ucx.framework.crawlers import (
    SchemaDeployer,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.framework.install_state import InstallState
from databricks.labs.ucx.framework.parallel import Threads
from databricks.labs.ucx.framework.tasks import _TASKS, Task
from databricks.labs.ucx.framework.tui import Prompts
from databricks.labs.ucx.framework.wheels import Wheels, find_project_root
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.hms_lineage import HiveMetastoreLineageEnabler
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation, Mount
from databricks.labs.ucx.hive_metastore.tables import Table, TableError
from databricks.labs.ucx.runtime import main
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.generic import WorkspaceObjectInfo
from databricks.labs.ucx.workspace_access.groups import ConfigureGroups, MigratedGroup

TAG_STEP = "step"
TAG_APP = "App"
WAREHOUSE_PREFIX = "Unity Catalog Migration"
NUM_USER_ATTEMPTS = 10  # number of attempts user gets at answering a question

EXTRA_TASK_PARAMS = {
    "job_id": "{{job_id}}",
    "run_id": "{{run_id}}",
    "parent_run_id": "{{parent_run_id}}",
}
DEBUG_NOTEBOOK = """
# Databricks notebook source
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
from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework import logger
from databricks.sdk import WorkspaceClient

logger._install()
logging.getLogger("databricks").setLevel("DEBUG")

cfg = WorkspaceConfig.from_file(Path("/Workspace{config_file}"))
ws = WorkspaceClient()

print(__version__)
"""

TEST_RUNNER_NOTEBOOK = """
# Databricks notebook source
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

logger = logging.getLogger(__name__)


def deploy_schema(sql_backend: SqlBackend, inventory_schema: str):
    from databricks.labs import ucx

    deployer = SchemaDeployer(sql_backend, inventory_schema, ucx)
    deployer.deploy_schema()
    table = functools.partial(deployer.deploy_table)
    Threads.strict(
        "deploy tables",
        [
            functools.partial(table, "azure_service_principals", AzureServicePrincipalInfo),
            functools.partial(table, "clusters", ClusterInfo),
            functools.partial(table, "global_init_scripts", GlobalInitScriptInfo),
            functools.partial(table, "jobs", JobInfo),
            functools.partial(table, "pipelines", PipelineInfo),
            functools.partial(table, "external_locations", ExternalLocation),
            functools.partial(table, "mounts", Mount),
            functools.partial(table, "grants", Grant),
            functools.partial(table, "groups", MigratedGroup),
            functools.partial(table, "tables", Table),
            functools.partial(table, "table_failures", TableError),
            functools.partial(table, "workspace_objects", WorkspaceObjectInfo),
            functools.partial(table, "permissions", Permissions),
        ],
    )
    deployer.deploy_view("objects", "queries/views/objects.sql")
    deployer.deploy_view("grant_detail", "queries/views/grant_detail.sql")


class WorkspaceInstaller:
    def __init__(
        self,
        ws: WorkspaceClient,
        *,
        prefix: str = "ucx",
        promtps: Prompts | None = None,
        wheels: Wheels | None = None,
        sql_backend: SqlBackend | None = None,
    ):
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)
        self._ws = ws
        self._prefix = prefix
        if not wheels:
            wheels = Wheels(ws, self._install_folder, __version__)
        self._sql_backend = sql_backend
        self._prompts = promtps
        self._wheels = wheels
        self._this_file = Path(__file__)
        self._dashboards: dict[str, str] = {}
        self._state = InstallState(ws, self._install_folder)
        self._install_override_clusters = None

    def run(self):
        logger.info(f"Installing UCX v{self._wheels.version()}")
        self._configure()
        self._run_configured()

    def _run_configured(self):
        self._install_spark_config_for_hms_lineage()
        self._create_dashboards()
        self._create_jobs()
        self._create_database()
        readme = f'{self.notebook_link(f"{self._install_folder}/README.py")}'
        msg = f"Installation completed successfully! Please refer to the {readme} notebook for next steps."
        logger.info(msg)

    def _create_database(self):
        if self._sql_backend is None:
            self._sql_backend = StatementExecutionBackend(self._ws, self.current_config.warehouse_id)
        deploy_schema(self._sql_backend, self.current_config.inventory_database)

    def _install_spark_config_for_hms_lineage(self):
        hms_lineage = HiveMetastoreLineageEnabler(ws=self._ws)
        logger.info(
            "Enabling HMS Lineage: "
            "HMS Lineage feature creates one system table named "
            "system.hms_to_uc_migration.table_access and "
            "helps in your migration process from HMS to UC by allowing you to programmatically query HMS "
            "lineage data."
        )
        logger.info("Checking if Global Init Script with Required Spark Config already exists and enabled.")
        gscript = hms_lineage.check_lineage_spark_config_exists()
        if gscript:
            if gscript.enabled:
                logger.info("Already exists and enabled. Skipped creating a new one.")
            elif not gscript.enabled and self._prompts:
                if self._prompts.confirm(
                    "Your Global Init Script with required spark config is disabled, Do you want to enable it?"
                ):
                    logger.info("Enabling Global Init Script...")
                    hms_lineage.enable_global_init_script(gscript)
                else:
                    logger.info("No change to Global Init Script is made.")
        elif not gscript and self._prompts:
            if self._prompts.confirm(
                "No Global Init Script with Required Spark Config exists, Do you want to create one?"
            ):
                logger.info("Creating Global Init Script...")
                hms_lineage.add_global_init_script()

    @staticmethod
    def run_for_config(
        ws: WorkspaceClient,
        config: WorkspaceConfig,
        *,
        prefix="ucx",
        wheels: Wheels | None = None,
        override_clusters: dict[str, str] | None = None,
        sql_backend: SqlBackend | None = None,
    ) -> "WorkspaceInstaller":
        workspace_installer = WorkspaceInstaller(ws, prefix=prefix, wheels=wheels, sql_backend=sql_backend)
        logger.info(f"Installing UCX v{workspace_installer._wheels.version()} on {ws.config.host}")
        workspace_installer._config = config  # type: ignore[has-type]
        workspace_installer._write_config(overwrite=False)
        workspace_installer.current_config.override_clusters = override_clusters
        # TODO: rather introduce a method `is_configured`, as we may want to reconfigure workspaces for some reason
        workspace_installer._run_configured()
        return workspace_installer

    def run_workflow(self, step: str):
        job_id = self._state.jobs[step]
        logger.debug(f"starting {step} job: {self._ws.config.host}#job/{job_id}")
        job_run_waiter = self._ws.jobs.run_now(job_id)
        try:
            job_run_waiter.result()
        except OperationFailed:
            # currently we don't have any good message from API, so we have to work around it.
            job_run = self._ws.jobs.get_run(job_run_waiter.run_id)
            messages = []
            assert job_run.tasks is not None
            for run_task in job_run.tasks:
                if not run_task.state:
                    continue
                if run_task.state.result_state == jobs.RunResultState.TIMEDOUT:
                    messages.append(f"{run_task.task_key}: The run was stopped after reaching the timeout")
                    continue
                if run_task.state.result_state != jobs.RunResultState.FAILED:
                    continue
                assert run_task.run_id is not None
                run_output = self._ws.jobs.get_run_output(run_task.run_id)
                if logger.isEnabledFor(logging.DEBUG):
                    if run_output and run_output.error_trace:
                        sys.stderr.write(run_output.error_trace)
                if run_output and run_output.error:
                    messages.append(f"{run_task.task_key}: {run_output.error}")
                else:
                    messages.append(f"{run_task.task_key}: output unavailable")
            assert job_run.state is not None
            assert job_run.state.state_message is not None
            msg = f'{job_run.state.state_message.rstrip(".")}: {", ".join(messages)}'
            raise OperationFailed(msg) from None

    def _create_dashboards(self):
        logger.info("Creating dashboards...")
        local_query_files = find_project_root() / "src/databricks/labs/ucx/queries"
        dash = DashboardFromFiles(
            self._ws,
            state=self._state,
            local_folder=local_query_files,
            remote_folder=f"{self._install_folder}/queries",
            name_prefix=self._name("UCX "),
            warehouse_id=self._warehouse_id,
            query_text_callback=self.current_config.replace_inventory_variable,
        )
        self._dashboards = dash.create_dashboards()

    @property
    def _warehouse_id(self) -> str:
        if self.current_config.warehouse_id is not None:
            return self.current_config.warehouse_id
        warehouses = [_ for _ in self._ws.warehouses.list() if _.warehouse_type == EndpointInfoWarehouseType.PRO]
        warehouse_id = self.current_config.warehouse_id
        if not warehouse_id and not warehouses:
            msg = "need either configured warehouse_id or an existing PRO SQL warehouse"
            raise ValueError(msg)
        if not warehouse_id:
            warehouse_id = warehouses[0].id
        self.current_config.warehouse_id = warehouse_id
        return warehouse_id

    @property
    def _my_username(self):
        if not hasattr(self, "_me"):
            self._me = self._ws.current_user.me()
            is_workspace_admin = any(g.display == "admins" for g in self._me.groups)
            if not is_workspace_admin:
                msg = "Current user is not a workspace admin"
                raise PermissionError(msg)
        return self._me.user_name

    @property
    def _short_name(self):
        if "@" in self._my_username:
            username = self._my_username.split("@")[0]
        else:
            username = self._me.display_name
        return username

    @property
    def _app(self):
        if not hasattr(self, "__app"):
            # return lower-cased alphanumeric + _ combination of prefix (UCX by default)
            # and shorter variant of username (or service principal) to serve as a unique
            # installation identifier on the workspace.
            app = f"{self._prefix}_{self._short_name}".lower()
            self.__app = "".join([_ if _.isalnum() else "_" for _ in app])
        return self.__app

    @property
    def _install_folder(self):
        return f"/Users/{self._my_username}/.{self._prefix}"

    @property
    def config_file(self):
        return f"{self._install_folder}/config.yml"

    @property
    def current_config(self) -> WorkspaceConfig:
        if hasattr(self, "_config"):
            return self._config  # type: ignore[has-type]
        with self._ws.workspace.download(self.config_file) as f:
            self._config = WorkspaceConfig.from_bytes(f.read())
        return self._config

    def _raw_previous_config(self):
        with self._ws.workspace.download(self.config_file) as f:
            return str(f.read())

    def _name(self, name: str) -> str:
        return f"[{self._prefix.upper()}] {name}"

    def _configure(self):
        ws_file_url = self.notebook_link(self.config_file)
        try:
            if "version: 1" in self._raw_previous_config():
                logger.info("old version detected, attempting to migrate to new config")
                self._config = self.current_config
                self._write_config(overwrite=True)
                return
            elif "version: 2" in self._raw_previous_config():
                logger.info(f"UCX is already configured. See {ws_file_url}")
                return
        except NotFound:
            pass
        if not self._prompts:
            raise RuntimeWarning("No Prompts instance found")
        logger.info("Please answer a couple of questions to configure Unity Catalog migration")
        inventory_database = self._prompts.question(
            "Inventory Database stored in hive_metastore", default="ucx", valid_regex=r"^\w+$"
        )

        def warehouse_type(_):
            return _.warehouse_type.value if not _.enable_serverless_compute else "SERVERLESS"

        pro_warehouses = {"[Create new PRO SQL warehouse]": "create_new"} | {
            f"{_.name} ({_.id}, {warehouse_type(_)}, {_.state.value})": _.id
            for _ in self._ws.warehouses.list()
            if _.warehouse_type == EndpointInfoWarehouseType.PRO
        }
        warehouse_id = self._prompts.choice_from_dict(
            "Select PRO or SERVERLESS SQL warehouse to run assessment dashboards on", pro_warehouses
        )
        if warehouse_id == "create_new":
            new_warehouse = self._ws.warehouses.create(
                name=f"{WAREHOUSE_PREFIX} {time.time_ns()}",
                spot_instance_policy=SpotInstancePolicy.COST_OPTIMIZED,
                warehouse_type=EndpointInfoWarehouseType.PRO,
                cluster_size="Small",
                max_num_clusters=1,
            )
            warehouse_id = new_warehouse.id

        configure_groups = ConfigureGroups(self._prompts)
        configure_groups.run()
        log_level = self._prompts.question("Log level", default="INFO").upper()
        num_threads = int(self._prompts.question("Number of threads", default="8", valid_number=True))

        # Checking for external HMS
        instance_profile = None
        spark_conf_dict = {}
        policies_with_external_hms = list(self._get_cluster_policies_with_external_hive_metastores())
        if len(policies_with_external_hms) > 0 and self._prompts.confirm(
            "We have identified one or more cluster policies set up for an external metastore"
            "Would you like to set UCX to connect to the external metastore?"
        ):
            logger.info("Setting up an external metastore")
            cluster_policies = {conf.name: conf.definition for conf in policies_with_external_hms}
            if len(cluster_policies) >= 1:
                cluster_policy = json.loads(self._prompts.choice_from_dict("Choose a cluster policy", cluster_policies))
                instance_profile, spark_conf_dict = self._get_ext_hms_conf_from_policy(cluster_policy)

        if self._prompts.confirm("Do you want to follow a policy to create clusters?"):
            cluster_policies_list = {f"{_.name} ({_.policy_id})": _.policy_id for _ in self._ws.cluster_policies.list()}
            custom_cluster_policy_id = self._prompts.choice_from_dict("Choose a cluster policy", cluster_policies_list)
        else:
            custom_cluster_policy_id = None

        self._config = WorkspaceConfig(
            inventory_database=inventory_database,
            workspace_group_regex=configure_groups.workspace_group_regex,
            workspace_group_replace=configure_groups.workspace_group_replace,
            account_group_regex=configure_groups.account_group_regex,
            group_match_by_external_id=configure_groups.group_match_by_external_id,
            include_group_names=configure_groups.include_group_names,
            renamed_group_prefix=configure_groups.renamed_group_prefix,
            warehouse_id=warehouse_id,
            log_level=log_level,
            num_threads=num_threads,
            instance_profile=instance_profile,
            spark_conf=spark_conf_dict,
            override_clusters=self._install_override_clusters,
            custom_cluster_policy_id=custom_cluster_policy_id,
        )

        self._write_config(overwrite=False)
        if self._prompts.confirm("Open config file in the browser and continue installing?"):
            webbrowser.open(ws_file_url)

    def _write_config(self, overwrite):
        try:
            self._ws.workspace.get_status(self._install_folder)
        except NotFound:
            logger.debug(f"Creating install folder: {self._install_folder}")
            self._ws.workspace.mkdirs(self._install_folder)

        config_bytes = yaml.dump(self._config.as_dict()).encode("utf8")
        logger.info(f"Creating configuration file: {self.config_file}")
        self._ws.workspace.upload(self.config_file, config_bytes, format=ImportFormat.AUTO, overwrite=overwrite)

    def _upload_wheel(self):
        with self._wheels:
            try:
                self._wheels.upload_to_dbfs()
            except PermissionDenied as err:
                if not self._prompts:
                    raise RuntimeWarning("no Prompts instance found") from err
                logger.warning(f"Uploading wheel file to DBFS failed, DBFS is probably write protected. {err}")
                configure_cluster_overrides = ConfigureClusterOverrides(self._ws, self._prompts.choice_from_dict)
                self.current_config.override_clusters = configure_cluster_overrides.configure()
            return self._wheels.upload_to_wsfs()

    def _create_jobs(self):
        if not self._state.jobs:
            for step, job_id in self._deployed_steps_pre_v06().items():
                self._state.jobs[step] = job_id
        logger.debug(f"Creating jobs from tasks in {main.__name__}")
        remote_wheel = self._upload_wheel()
        desired_steps = {t.workflow for t in _TASKS.values() if t.cloud_compatible(self._ws.config)}
        wheel_runner = None

        if self.current_config.override_clusters:
            wheel_runner = self._upload_wheel_runner(remote_wheel)
        for step_name in desired_steps:
            settings = self._job_settings(step_name, remote_wheel)
            if self.current_config.override_clusters:
                settings = self._apply_cluster_overrides(settings, self.current_config.override_clusters, wheel_runner)
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
        self._create_readme()
        self._create_debug(remote_wheel)

    def _deploy_workflow(self, step_name: str, settings):
        if step_name in self._state.jobs:
            try:
                job_id = self._state.jobs[step_name]
                logger.info(f"Updating configuration for step={step_name} job_id={job_id}")
                return self._ws.jobs.reset(job_id, jobs.JobSettings(**settings))
            except InvalidParameterValue:
                del self._state.jobs[step_name]
                logger.warning(f"step={step_name} does not exist anymore for some reason")
                return self._deploy_workflow(step_name, settings)
        logger.info(f"Creating new job configuration for step={step_name}")
        job_id = self._ws.jobs.create(**settings).job_id
        self._state.jobs[step_name] = job_id

    def _deployed_steps_pre_v06(self):
        deployed_steps = {}
        logger.debug(f"Fetching all jobs to determine already deployed steps for app={self._app}")
        for j in self._ws.jobs.list():
            tags = j.settings.tags
            if tags is None:
                continue
            if tags.get(TAG_APP, None) != self._app:
                continue
            step = tags.get(TAG_STEP, "_")
            deployed_steps[step] = j.job_id
        return deployed_steps

    @staticmethod
    def _sorted_tasks() -> list[Task]:
        return sorted(_TASKS.values(), key=lambda x: x.task_id)

    @classmethod
    def _step_list(cls) -> list[str]:
        step_list = []
        for task in cls._sorted_tasks():
            if task.workflow not in step_list:
                step_list.append(task.workflow)
        return step_list

    def _create_readme(self):
        md = [
            "# UCX - The Unity Catalog Migration Assistant",
            f'To troubleshoot, see [debug notebook]({self.notebook_link(f"{self._install_folder}/DEBUG.py")}).\n',
            "Here are the URLs and descriptions of workflows that trigger various stages of migration.",
            "All jobs are defined with necessary cluster configurations and DBR versions.\n",
        ]
        for step_name in self._step_list():
            if step_name not in self._state.jobs:
                logger.warning(f"Skipping step '{step_name}' since it was not deployed.")
                continue
            job_id = self._state.jobs[step_name]
            dashboard_link = ""
            dashboards_per_step = [d for d in self._dashboards.keys() if d.startswith(step_name)]
            for dash in dashboards_per_step:
                if len(dashboard_link) == 0:
                    dashboard_link += "Go to the one of the following dashboards after running the job:\n"
                first, second = dash.replace("_", " ").title().split()
                dashboard_url = f"{self._ws.config.host}/sql/dashboards/{self._dashboards[dash]}"
                dashboard_link += f"  - [{first} ({second}) dashboard]({dashboard_url})\n"
            job_link = f"[{self._name(step_name)}]({self._ws.config.host}#job/{job_id})"
            md.append("---\n\n")
            md.append(f"## {job_link}\n\n")
            md.append(f"{dashboard_link}")
            md.append("\nThe workflow consists of the following separate tasks:\n\n")
            for t in self._sorted_tasks():
                if t.workflow != step_name:
                    continue
                doc = self.current_config.replace_inventory_variable(t.doc)
                md.append(f"### `{t.name}`\n\n")
                md.append(f"{doc}\n")
                md.append("\n\n")
        preamble = ["# Databricks notebook source", "# MAGIC %md"]
        intro = "\n".join(preamble + [f"# MAGIC {line}" for line in md])
        path = f"{self._install_folder}/README.py"
        self._ws.workspace.upload(path, intro.encode("utf8"), overwrite=True)
        url = self.notebook_link(path)
        logger.info(f"Created README notebook with job overview: {url}")
        if self._prompts and self._prompts.confirm("Open job overview in README notebook in your home directory?"):
            webbrowser.open(url)

    def _replace_inventory_variable(self, text: str) -> str:
        return text.replace("$inventory", f"hive_metastore.{self.current_config.inventory_database}")

    def _create_debug(self, remote_wheel: str):
        readme_link = self.notebook_link(f"{self._install_folder}/README.py")
        job_links = ", ".join(
            f"[{self._name(step_name)}]({self._ws.config.host}#job/{job_id})"
            for step_name, job_id in self._state.jobs.items()
        )
        path = f"{self._install_folder}/DEBUG.py"
        logger.debug(f"Created debug notebook: {self.notebook_link(path)}")
        content = DEBUG_NOTEBOOK.format(
            remote_wheel=remote_wheel, readme_link=readme_link, job_links=job_links, config_file=self.config_file
        ).encode("utf8")
        self._ws.workspace.upload(path, content, overwrite=True)  # type: ignore[arg-type]

    def notebook_link(self, path: str) -> str:
        return f"{self._ws.config.host}/#workspace{path}"

    def _job_settings(self, step_name: str, remote_wheel: str):
        email_notifications = None
        if not self.current_config.override_clusters and "@" in self._my_username:
            # set email notifications only if we're running the real
            # installation and not the integration test.
            email_notifications = jobs.JobEmailNotifications(
                on_success=[self._my_username], on_failure=[self._my_username]
            )
        tasks = sorted(
            [t for t in _TASKS.values() if t.workflow == step_name],
            key=lambda _: _.name,
        )
        version = self._wheels.version()
        version = version if not self._ws.config.is_gcp else version.replace("+", "-")
        return {
            "name": self._name(step_name),
            "tags": {TAG_APP: self._app, "version": f"v{version}"},
            "job_clusters": self._job_clusters({t.job_cluster for t in tasks}),
            "email_notifications": email_notifications,
            "tasks": [self._job_task(task, remote_wheel) for task in tasks],
        }

    def _upload_wheel_runner(self, remote_wheel: str):
        # TODO: we have to be doing this workaround until ES-897453 is solved in the platform
        path = f"{self._install_folder}/wheels/wheel-test-runner-{self._wheels.version()}.py"
        logger.debug(f"Created runner notebook: {self.notebook_link(path)}")
        py = TEST_RUNNER_NOTEBOOK.format(remote_wheel=remote_wheel, config_file=self.config_file).encode("utf8")
        self._ws.workspace.upload(path, py, overwrite=True)  # type: ignore[arg-type]
        return path

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

    def _job_task(self, task: Task, remote_wheel: str) -> jobs.Task:
        jobs_task = jobs.Task(
            task_key=task.name,
            job_cluster_key=task.job_cluster,
            depends_on=[jobs.TaskDependency(task_key=d) for d in _TASKS[task.name].dependencies()],
        )
        if task.dashboard:
            return self._job_dashboard_task(jobs_task, task)
        if task.notebook:
            return self._job_notebook_task(jobs_task, task)
        return self._job_wheel_task(jobs_task, task, remote_wheel)

    def _job_dashboard_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        assert task.dashboard is not None
        return replace(
            jobs_task,
            job_cluster_key=None,
            sql_task=jobs.SqlTask(
                warehouse_id=self._warehouse_id,
                dashboard=jobs.SqlTaskDashboard(dashboard_id=self._dashboards[task.dashboard]),
            ),
        )

    def _job_notebook_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        assert task.notebook is not None
        local_notebook = self._this_file.parent / task.notebook
        remote_notebook = f"{self._install_folder}/{local_notebook.name}"
        with local_notebook.open("rb") as f:
            self._ws.workspace.upload(remote_notebook, f, overwrite=True)
        return replace(
            jobs_task,
            notebook_task=jobs.NotebookTask(
                notebook_path=remote_notebook,
                # ES-872211: currently, we cannot read WSFS files from Scala context
                base_parameters={
                    "task": task.name,
                    "config": f"/Workspace{self.config_file}",
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
                named_parameters={"task": task.name, "config": f"/Workspace{self.config_file}"} | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_clusters(self, names: set[str]):
        clusters = []
        spark_conf = {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        }
        if self.current_config.spark_conf is not None:
            spark_conf = spark_conf | self.current_config.spark_conf
        spec = self._cluster_node_type(
            compute.ClusterSpec(
                spark_version=self._ws.clusters.select_spark_version(latest=True),
                data_security_mode=compute.DataSecurityMode.LEGACY_SINGLE_USER,
                spark_conf=spark_conf,
                custom_tags={"ResourceClass": "SingleNode"},
                num_workers=0,
            )
        )
        if self._config.custom_cluster_policy_id is not None:
            spec = replace(spec, policy_id=self._config.custom_cluster_policy_id)
        if self._ws.config.is_aws and spec.aws_attributes is not None:
            # TODO: we might not need spec.aws_attributes, if we have a cluster policy
            aws_attributes = replace(spec.aws_attributes, instance_profile_arn=self._config.instance_profile)
            spec = replace(spec, aws_attributes=aws_attributes)
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

    def _cluster_node_type(self, spec: compute.ClusterSpec) -> compute.ClusterSpec:
        cfg = self.current_config
        valid_node_type = False
        if cfg.custom_cluster_policy_id is not None:
            if self._check_policy_has_instance_pool(cfg.custom_cluster_policy_id):
                valid_node_type = True
        if not valid_node_type:
            if cfg.instance_pool_id is not None:
                return replace(spec, instance_pool_id=cfg.instance_pool_id)
            spec = replace(spec, node_type_id=self._ws.clusters.select_node_type(local_disk=True))
        if self._ws.config.is_aws:
            return replace(spec, aws_attributes=compute.AwsAttributes(availability=compute.AwsAvailability.ON_DEMAND))
        if self._ws.config.is_azure:
            return replace(
                spec, azure_attributes=compute.AzureAttributes(availability=compute.AzureAvailability.ON_DEMAND_AZURE)
            )
        return replace(spec, gcp_attributes=compute.GcpAttributes(availability=compute.GcpAvailability.ON_DEMAND_GCP))

    def _instance_profiles(self):
        return {"No Instance Profile": None} | {
            profile.instance_profile_arn: profile.instance_profile_arn for profile in self._ws.instance_profiles.list()
        }

    def _get_cluster_policies_with_external_hive_metastores(self):
        for policy in self._ws.cluster_policies.list():
            def_json = json.loads(policy.definition)
            glue_node = def_json.get("spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled")
            if glue_node is not None and glue_node.get("value") == "true":
                yield policy
                continue
            for key in def_json.keys():
                if key.startswith("spark_config.spark.sql.hive.metastore"):
                    yield policy
                    break

    def _check_policy_has_instance_pool(self, policy_id):
        policy = self._ws.cluster_policies.get(policy_id=policy_id)
        def_json = json.loads(policy.definition)
        instance_pool = def_json.get("instance_pool_id")
        if instance_pool is not None:
            return True
        else:
            return False

    @staticmethod
    def _get_ext_hms_conf_from_policy(cluster_policy):
        spark_conf_dict = {}
        instance_profile = None
        if cluster_policy.get("aws_attributes.instance_profile_arn") is not None:
            instance_profile = cluster_policy.get("aws_attributes.instance_profile_arn").get("value")
            logger.info(f"Instance Profile is Set to {instance_profile}")
        for key in cluster_policy.keys():
            if (
                key.startswith("spark_conf.sql.hive.metastore")
                or key.startswith("spark_conf.spark.hadoop.javax.jdo.option")
                or key.startswith("spark_conf.spark.databricks.hive.metastore")
                or key.startswith("spark_conf.spark.hadoop.hive.metastore.glue")
            ):
                spark_conf_dict[key[11:]] = cluster_policy[key]["value"]
        return instance_profile, spark_conf_dict

    def latest_job_status(self) -> list[dict]:
        latest_status = []
        for step, job_id in self._state.jobs.items():
            try:
                job_runs = list(self._ws.jobs.list_runs(job_id=job_id, limit=1))
                state = job_runs[0].state
                result_state = state.result_state if state else None
                latest_status.append(
                    {
                        "step": step,
                        "state": "UNKNOWN" if not job_runs else str(result_state),
                        "started": "<never run>" if not job_runs else job_runs[0].start_time,
                    }
                )
            except InvalidParameterValue as e:
                logger.warning(f"skipping {step}: {e}")
                continue
        return latest_status

    def uninstall(self):
        if self._prompts and not self._prompts.confirm(
            "Do you want to uninstall ucx from the workspace too, this would "
            "remove ucx project folder, dashboards, queries and jobs"
        ):
            return
        logger.info(f"Deleting UCX v{self._wheels.version()} from {self._ws.config.host}")
        try:
            self._ws.workspace.get_status(self.config_file)
            self._ws.workspace.get_status(self._install_folder)
            self._ws.workspace.get_status(self._state._state_file)
        except NotFound:
            logger.error(
                f"Check if {self._install_folder} is present along with {self.config_file} and "
                f"{self._state._state_file}."
            )
            return
        self._remove_database()
        self._remove_jobs()
        self._remove_warehouse()
        self._remove_install_folder()
        logger.info("UnInstalling UCX complete")

    def _remove_database(self):
        if self._prompts and not self._prompts.confirm(
            f"Do you want to delete the inventory database {self.current_config.inventory_database} too?"
        ):
            return
        logger.info(f"Deleting inventory database {self.current_config.inventory_database}")
        if self._sql_backend is None:
            self._sql_backend = StatementExecutionBackend(self._ws, self.current_config.warehouse_id)
        deployer = SchemaDeployer(self._sql_backend, self.current_config.inventory_database, Any)
        deployer.delete_schema()

    def _remove_jobs(self):
        logger.info("Deleting jobs")
        if not self._state.jobs:
            logger.error("No jobs present or jobs already deleted")
            return
        for step_name, job_id in self._state.jobs.items():
            try:
                logger.info(f"Deleting {step_name} job_id={job_id}.")
                self._ws.jobs.delete(job_id)
            except InvalidParameterValue:
                logger.error(f"Already deleted: {step_name} job_id={job_id}.")
                continue

    def _remove_warehouse(self):
        try:
            warehouse_name = self._ws.warehouses.get(self.current_config.warehouse_id).name
            if warehouse_name.startswith(WAREHOUSE_PREFIX):
                logger.info("Deleting warehouse_name.")
                self._ws.warehouses.delete(id=self.current_config.warehouse_id)
        except InvalidParameterValue:
            logger.error("Error accessing warehouse details")

    def _remove_install_folder(self):
        try:
            logger.info(f"Deleting install folder {self._install_folder}.")
            self._ws.workspace.delete(path=self._install_folder, recursive=True)
        except InvalidParameterValue:
            logger.error("Error deleting install folder")

    def validate_step(self, step: str) -> bool:
        job_id = self._state.jobs[step]
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
                self._ws.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)
                run_new_state = self._ws.jobs.get_run(run_id=run.run_id).state
                return run_new_state is not None and run_new_state.result_state == RunResultState.SUCCESS
        return False

    def validate_and_run(self, step: str):
        if not self.validate_step(step):
            self.run_workflow(step)


if __name__ == "__main__":
    ws = WorkspaceClient(product="ucx", product_version=__version__)
    logger.setLevel("INFO")
    installer = WorkspaceInstaller(ws, promtps=Prompts())
    installer.run()

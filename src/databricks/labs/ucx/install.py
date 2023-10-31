import datetime
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import webbrowser
from dataclasses import replace
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import OperationFailed
from databricks.sdk.mixins.compute import SemVer
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.sql import EndpointInfoWarehouseType, SpotInstancePolicy
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import GroupsConfig, WorkspaceConfig
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.framework.tasks import _TASKS, Task
from databricks.labs.ucx.hive_metastore.hms_lineage import HiveMetastoreLineageEnabler
from databricks.labs.ucx.runtime import main

TAG_STEP = "step"
TAG_APP = "App"
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


class WorkspaceInstaller:
    def __init__(self, ws: WorkspaceClient, *, prefix: str = "ucx", promtps: bool = True):
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)
        self._ws = ws
        self._prefix = prefix
        self._prompts = promtps
        self._this_file = Path(__file__)
        self._override_clusters = None
        self._dashboards = {}

    def run(self):
        logger.info(f"Installing UCX v{self._version}")
        self._configure()
        self._run_configured()

    def _run_configured(self):
        self._install_spark_config_for_hms_lineage()
        self._create_dashboards()
        self._create_jobs()
        readme = f'{self._notebook_link(f"{self._install_folder}/README.py")}'
        msg = f"Installation completed successfully! Please refer to the {readme} notebook for next steps."
        logger.info(msg)

    def _install_spark_config_for_hms_lineage(self):
        if (
            self._prompts
            and self._question(
                "Do you want to enable HMS Lineage "
                "(HMS Lineage feature creates one system table named "
                "system.hms_to_uc_migration.table_access and "
                "helps in your migration process from HMS to UC by allowing you to programmatically query HMS "
                "lineage data)",
                default="yes",
            )
            == "yes"
        ):
            hms_lineage = HiveMetastoreLineageEnabler(ws=self._ws)
            hms_lineage.add_spark_config_for_hms_lineage()

    @staticmethod
    def run_for_config(
        ws: WorkspaceClient, config: WorkspaceConfig, *, prefix="ucx", override_clusters: dict[str, str] | None = None
    ) -> "WorkspaceInstaller":
        logger.info(f"Installing UCX v{__version__} on {ws.config.host}")
        workspace_installer = WorkspaceInstaller(ws, prefix=prefix, promtps=False)
        logger.info(f"Installing UCX v{workspace_installer._version} on {ws.config.host}")
        workspace_installer._config = config
        workspace_installer._write_config()
        workspace_installer._override_clusters = override_clusters
        # TODO: rather introduce a method `is_configured`, as we may want to reconfigure workspaces for some reason
        workspace_installer._run_configured()
        return workspace_installer

    def run_workflow(self, step: str):
        job_id = self._deployed_steps[step]
        logger.debug(f"starting {step} job: {self._ws.config.host}#job/{job_id}")
        job_run_waiter = self._ws.jobs.run_now(job_id)
        try:
            job_run_waiter.result()
        except OperationFailed:
            # currently we don't have any good message from API, so we have to work around it.
            job_run = self._ws.jobs.get_run(job_run_waiter.run_id)
            messages = []
            for run_task in job_run.tasks:
                if run_task.state.result_state == jobs.RunResultState.TIMEDOUT:
                    messages.append(f"{run_task.task_key}: The run was stopped after reaching the timeout")
                    continue
                if run_task.state.result_state != jobs.RunResultState.FAILED:
                    continue
                run_output = self._ws.jobs.get_run_output(run_task.run_id)
                if logger.isEnabledFor(logging.DEBUG):
                    sys.stderr.write(run_output.error_trace)
                messages.append(f"{run_task.task_key}: {run_output.error}")
            msg = f'{job_run.state.state_message.rstrip(".")}: {", ".join(messages)}'
            raise OperationFailed(msg) from None

    def _create_dashboards(self):
        logger.info("Creating dashboards...")
        local_query_files = self._find_project_root() / "src/databricks/labs/ucx/queries"
        dash = DashboardFromFiles(
            self._ws,
            local_folder=local_query_files,
            remote_folder=f"{self._install_folder}/queries",
            name_prefix=self._name("UCX "),
            warehouse_id=self._warehouse_id,
            query_text_callback=self._current_config.replace_inventory_variable,
        )
        self._dashboards = dash.create_dashboards()

    @property
    def _warehouse_id(self) -> str:
        if self._current_config.warehouse_id is not None:
            return self._current_config.warehouse_id
        warehouses = [_ for _ in self._ws.warehouses.list() if _.warehouse_type == EndpointInfoWarehouseType.PRO]
        warehouse_id = self._current_config.warehouse_id
        if not warehouse_id and not warehouses:
            msg = "need either configured warehouse_id or an existing PRO SQL warehouse"
            raise ValueError(msg)
        if not warehouse_id:
            warehouse_id = warehouses[0].id
        self._current_config.warehouse_id = warehouse_id
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
    def _config_file(self):
        return f"{self._install_folder}/config.yml"

    @property
    def _current_config(self):
        if hasattr(self, "_config"):
            return self._config
        with self._ws.workspace.download(self._config_file) as f:
            self._config = WorkspaceConfig.from_bytes(f.read())
        return self._config

    def _name(self, name: str) -> str:
        return f"[{self._prefix.upper()}][{self._short_name}] {name}"

    def _configure_inventory_database(self):
        counter = 0
        inventory_database = None
        while True:
            inventory_database = self._question("Inventory Database stored in hive_metastore", default="ucx")
            if re.match(r"^\w+$", inventory_database):
                break
            else:
                print(f"{inventory_database} is not a valid database name")
                counter = counter + 1
                if counter > NUM_USER_ATTEMPTS:
                    msg = "Exceeded max tries to get a valid database name, try again later."
                    raise SystemExit(msg)
        return inventory_database

    def _configure(self):
        ws_file_url = self._notebook_link(self._config_file)
        try:
            self._ws.workspace.get_status(self._config_file)
            logger.info(f"UCX is already configured. See {ws_file_url}")
            if self._prompts and self._question("Open config file in the browser", default="yes") == "yes":
                webbrowser.open(ws_file_url)
            return
        except DatabricksError as err:
            if err.error_code != "RESOURCE_DOES_NOT_EXIST":
                raise err

        logger.info("Please answer a couple of questions to configure Unity Catalog migration")
        inventory_database = self._configure_inventory_database()

        def warehouse_type(_):
            return _.warehouse_type.value if not _.enable_serverless_compute else "SERVERLESS"

        pro_warehouses = {"[Create new PRO SQL warehouse]": "create_new"} | {
            f"{_.name} ({_.id}, {warehouse_type(_)}, {_.state.value})": _.id
            for _ in self._ws.warehouses.list()
            if _.warehouse_type == EndpointInfoWarehouseType.PRO
        }
        warehouse_id = self._choice_from_dict(
            "Select PRO or SERVERLESS SQL warehouse to run assessment dashboards on", pro_warehouses
        )
        if warehouse_id == "create_new":
            new_warehouse = self._ws.warehouses.create(
                name="Unity Catalog Migration",
                spot_instance_policy=SpotInstancePolicy.COST_OPTIMIZED,
                warehouse_type=EndpointInfoWarehouseType.PRO,
                cluster_size="Small",
                max_num_clusters=1,
            )
            warehouse_id = new_warehouse.id

        selected_groups = self._question(
            "Comma-separated list of workspace group names to migrate. If not specified, we'll use all "
            "account-level groups with matching names to workspace-level groups.",
            default="<ALL>",
        )
        backup_group_prefix = self._question("Backup prefix", default="db-temp-")
        log_level = self._question("Log level", default="INFO").upper()
        num_threads = int(self._question("Number of threads", default="8"))
        groups_config_args = {
            "backup_group_prefix": backup_group_prefix,
        }
        if selected_groups != "<ALL>":
            groups_config_args["selected"] = [x.strip() for x in selected_groups.split(",")]
        else:
            groups_config_args["auto"] = True

        # Checking for external HMS
        instance_profile = None
        spark_conf_dict = {}
        if self._prompts:
            policies_with_external_hms = list(self._get_cluster_policies_with_external_hive_metastores())
            if (
                len(policies_with_external_hms) > 0
                and self._question(
                    "We have identified one or more cluster policies set up for an external metastore. "
                    "Would you like to set UCX to connect to the external metastore.",
                    default="no",
                )
                == "yes"
            ):
                logger.info("Setting up an external metastore")
                cluster_policies = {conf.name: conf.definition for conf in policies_with_external_hms}
                if len(cluster_policies) >= 1:
                    cluster_policy = json.loads(
                        self._choice_from_dict("Select a Cluster Policy from The List", cluster_policies)
                    )
                    instance_profile, spark_conf_dict = self._get_ext_hms_conf_from_policy(cluster_policy)

        self._config = WorkspaceConfig(
            inventory_database=inventory_database,
            groups=GroupsConfig(**groups_config_args),
            warehouse_id=warehouse_id,
            log_level=log_level,
            num_threads=num_threads,
            instance_profile=instance_profile,
            spark_conf=spark_conf_dict,
        )

        self._write_config()
        msg = "Open config file in the browser and continue installing?"
        if self._prompts and self._question(msg, default="yes") == "yes":
            webbrowser.open(ws_file_url)

    def _write_config(self):
        try:
            self._ws.workspace.get_status(self._install_folder)
        except DatabricksError as err:
            if err.error_code != "RESOURCE_DOES_NOT_EXIST":
                raise err
            logger.debug(f"Creating install folder: {self._install_folder}")
            self._ws.workspace.mkdirs(self._install_folder)

        config_bytes = yaml.dump(self._config.as_dict()).encode("utf8")
        logger.info(f"Creating configuration file: {self._config_file}")
        self._ws.workspace.upload(self._config_file, config_bytes, format=ImportFormat.AUTO)

    def _create_jobs(self):
        logger.debug(f"Creating jobs from tasks in {main.__name__}")
        remote_wheel = self._upload_wheel()
        self._deployed_steps = self._deployed_steps()
        desired_steps = {t.workflow for t in _TASKS.values()}
        wheel_runner = None

        if self._override_clusters:
            wheel_runner = self._upload_wheel_runner(remote_wheel)
        for step_name in desired_steps:
            settings = self._job_settings(step_name, remote_wheel)
            if self._override_clusters:
                settings = self._apply_cluster_overrides(settings, self._override_clusters, wheel_runner)
            if step_name in self._deployed_steps:
                job_id = self._deployed_steps[step_name]
                logger.info(f"Updating configuration for step={step_name} job_id={job_id}")
                self._ws.jobs.reset(job_id, jobs.JobSettings(**settings))
            else:
                logger.info(f"Creating new job configuration for step={step_name}")
                self._deployed_steps[step_name] = self._ws.jobs.create(**settings).job_id

        for step_name, job_id in self._deployed_steps.items():
            if step_name not in desired_steps:
                logger.info(f"Removing job_id={job_id}, as it is no longer needed")
                self._ws.jobs.delete(job_id)

        self._create_readme()
        self._create_debug(remote_wheel)

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
            f'To troubleshoot, see [debug notebook]({self._notebook_link(f"{self._install_folder}/DEBUG.py")}).\n',
            "Here are the URLs and descriptions of workflows that trigger various stages of migration.",
            "All jobs are defined with necessary cluster configurations and DBR versions.\n",
        ]
        for step_name in self._step_list():
            if step_name not in self._deployed_steps:
                logger.warning(f"Skipping step '{step_name}' since it was not deployed.")
                continue
            job_id = self._deployed_steps[step_name]
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
                doc = self._current_config.replace_inventory_variable(t.doc)
                md.append(f"### `{t.name}`\n\n")
                md.append(f"{doc}\n")
                md.append("\n\n")
        preamble = ["# Databricks notebook source", "# MAGIC %md"]
        intro = "\n".join(preamble + [f"# MAGIC {line}" for line in md])
        path = f"{self._install_folder}/README.py"
        self._ws.workspace.upload(path, intro.encode("utf8"), overwrite=True)
        url = self._notebook_link(path)
        logger.info(f"Created README notebook with job overview: {url}")
        msg = "Open job overview in README notebook in your home directory ?"
        if self._prompts and self._question(msg, default="yes") == "yes":
            webbrowser.open(url)

    def _replace_inventory_variable(self, text: str) -> str:
        return text.replace("$inventory", f"hive_metastore.{self._current_config.inventory_database}")

    def _create_debug(self, remote_wheel: str):
        readme_link = self._notebook_link(f"{self._install_folder}/README.py")
        job_links = ", ".join(
            f"[{self._name(step_name)}]({self._ws.config.host}#job/{job_id})"
            for step_name, job_id in self._deployed_steps.items()
        )
        path = f"{self._install_folder}/DEBUG.py"
        logger.debug(f"Created debug notebook: {self._notebook_link(path)}")
        self._ws.workspace.upload(
            path,
            DEBUG_NOTEBOOK.format(
                remote_wheel=remote_wheel, readme_link=readme_link, job_links=job_links, config_file=self._config_file
            ).encode("utf8"),
            overwrite=True,
        )

    def _notebook_link(self, path: str) -> str:
        return f"{self._ws.config.host}/#workspace{path}"

    def _choice_from_dict(self, text: str, choices: dict[str, Any]) -> Any:
        key = self._choice(text, list(choices.keys()))
        return choices[key]

    def _choice(self, text: str, choices: list[Any], *, max_attempts: int = 10) -> str:
        if not self._prompts:
            return "any"
        choices = sorted(choices, key=str.casefold)
        numbered = "\n".join(f"\033[1m[{i}]\033[0m \033[36m{v}\033[0m" for i, v in enumerate(choices))
        prompt = f"\033[1m{text}\033[0m\n{numbered}\nEnter a number between 0 and {len(choices)-1}: "
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            res = input(prompt)
            try:
                res = int(res)
            except ValueError:
                print(f"\033[31m[ERROR] Invalid number: {res}\033[0m\n")
                continue
            if res >= len(choices) or res < 0:
                print(f"\033[31m[ERROR] Out of range: {res}\033[0m\n")
                continue
            return choices[res]
        msg = f"cannot get answer within {max_attempts} attempt"
        raise ValueError(msg)

    @staticmethod
    def _question(text: str, *, default: str | None = None) -> str:
        default_help = "" if default is None else f"\033[36m (default: {default})\033[0m"
        prompt = f"\033[1m{text}{default_help}: \033[0m"
        res = None
        while not res:
            res = input(prompt)
            if not res and default is not None:
                return default
        return res

    def _upload_wheel(self) -> str:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_wheel = self._build_wheel(tmp_dir)
            remote_wheel = f"{self._install_folder}/wheels/{local_wheel.name}"
            remote_dirname = os.path.dirname(remote_wheel)
            with local_wheel.open("rb") as f:
                self._ws.dbfs.mkdirs(remote_dirname)
                logger.info(f"Uploading wheel to dbfs:{remote_wheel}")
                self._ws.dbfs.upload(remote_wheel, f, overwrite=True)
            with local_wheel.open("rb") as f:
                self._ws.workspace.mkdirs(remote_dirname)
                logger.info(f"Uploading wheel to /Workspace{remote_wheel}")
                self._ws.workspace.upload(remote_wheel, f, overwrite=True, format=ImportFormat.AUTO)
        return remote_wheel

    def _job_settings(self, step_name: str, dbfs_path: str):
        email_notifications = None
        if not self._override_clusters and "@" in self._my_username:
            # set email notifications only if we're running the real
            # installation and not the integration test.
            email_notifications = jobs.JobEmailNotifications(
                on_success=[self._my_username], on_failure=[self._my_username]
            )
        tasks = sorted([t for t in _TASKS.values() if t.workflow == step_name], key=lambda _: _.name)
        version = self._version if not self._ws.config.is_gcp else self._version.replace("+", "-")
        return {
            "name": self._name(step_name),
            "tags": {TAG_APP: self._app, TAG_STEP: step_name, "version": f"v{version}"},
            "job_clusters": self._job_clusters({t.job_cluster for t in tasks}),
            "email_notifications": email_notifications,
            "tasks": [self._job_task(task, dbfs_path) for task in tasks],
        }

    @staticmethod
    def _apply_cluster_overrides(settings: dict[str, any], overrides: dict[str, str]) -> dict:
        settings["job_clusters"] = [_ for _ in settings["job_clusters"] if _.job_cluster_key not in overrides]
        for job_task in settings["tasks"]:
            if job_task.job_cluster_key is None:
                continue
            if job_task.job_cluster_key in overrides:
                job_task.existing_cluster_id = overrides[job_task.job_cluster_key]
                job_task.job_cluster_key = None
        return settings

    def _upload_wheel_runner(self, remote_wheel: str):
        # TODO: we have to be doing this workaround until ES-897453 is solved in the platform
        path = f"{self._install_folder}/wheels/wheel-test-runner-{self._version}.py"
        logger.debug(f"Created runner notebook: {self._notebook_link(path)}")
        py = TEST_RUNNER_NOTEBOOK.format(remote_wheel=remote_wheel, config_file=self._config_file).encode("utf8")
        self._ws.workspace.upload(path, py, overwrite=True)
        return path

    @staticmethod
    def _apply_cluster_overrides(settings: dict[str, any], overrides: dict[str, str], wheel_runner: str) -> dict:
        settings["job_clusters"] = [_ for _ in settings["job_clusters"] if _.job_cluster_key not in overrides]
        for job_task in settings["tasks"]:
            if job_task.job_cluster_key is None:
                continue
            if job_task.job_cluster_key in overrides:
                job_task.existing_cluster_id = overrides[job_task.job_cluster_key]
                job_task.job_cluster_key = None
            if job_task.python_wheel_task is not None:
                job_task.python_wheel_task = None
                params = {"task": job_task.task_key} | EXTRA_TASK_PARAMS
                job_task.notebook_task = jobs.NotebookTask(notebook_path=wheel_runner, base_parameters=params)
        return settings

    def _job_task(self, task: Task, dbfs_path: str) -> jobs.Task:
        jobs_task = jobs.Task(
            task_key=task.name,
            job_cluster_key=task.job_cluster,
            depends_on=[jobs.TaskDependency(task_key=d) for d in _TASKS[task.name].depends_on],
        )
        if task.dashboard:
            return self._job_dashboard_task(jobs_task, task)
        if task.notebook:
            return self._job_notebook_task(jobs_task, task)
        return self._job_wheel_task(jobs_task, task, dbfs_path)

    def _job_dashboard_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        return replace(
            jobs_task,
            job_cluster_key=None,
            sql_task=jobs.SqlTask(
                warehouse_id=self._warehouse_id,
                dashboard=jobs.SqlTaskDashboard(dashboard_id=self._dashboards[task.dashboard]),
            ),
        )

    def _job_notebook_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
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
                    "inventory_database": self._current_config.inventory_database,
                    "task": task.name,
                    "config": f"/Workspace{self._config_file}",
                }
                | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_wheel_task(self, jobs_task: jobs.Task, task: Task, dbfs_path: str) -> jobs.Task:
        return replace(
            jobs_task,
            libraries=[compute.Library(whl=f"dbfs:{dbfs_path}")],
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
        spec = self._cluster_node_type(
            compute.ClusterSpec(
                spark_version=self._ws.clusters.select_spark_version(latest=True),
                data_security_mode=compute.DataSecurityMode.NONE,
                spark_conf=spark_conf,
                custom_tags={"ResourceClass": "SingleNode"},
                num_workers=0,
            )
        )
        if self._ws.config.is_aws:
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

    @property
    def _version(self):
        if hasattr(self, "__version"):
            return self.__version
        project_root = self._find_project_root()
        if not (project_root / ".git/config").exists():
            # normal install, downloaded releases won't have the .git folder
            return __version__
        try:
            out = subprocess.run(["git", "describe", "--tags"], stdout=subprocess.PIPE, check=True)  # noqa S607
            git_detached_version = out.stdout.decode("utf8")
            dv = SemVer.parse(git_detached_version)
            datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            # new commits on main branch since the last tag
            new_commits = dv.pre_release.split("-")[0]
            # show that it's a version different from the released one in stats
            bump_patch = dv.patch + 1
            # create something that is both https://semver.org and https://peps.python.org/pep-0440/
            semver_and_pep0440 = f"{dv.major}.{dv.minor}.{bump_patch}+{new_commits}{datestamp}"
            # validate the semver
            SemVer.parse(semver_and_pep0440)
            self.__version = semver_and_pep0440
            return semver_and_pep0440
        except Exception as err:
            msg = (
                f"Cannot determine unreleased version. Please report this error "
                f"message that you see on https://github.com/databrickslabs/ucx/issues/new. "
                f"Meanwhile, download, unpack, and install the latest released version from "
                f"https://github.com/databrickslabs/ucx/releases. Original error is: {err!s}"
            )
            raise OSError(msg) from None

    def _build_wheel(self, tmp_dir: str, *, verbose: bool = False):
        """Helper to build the wheel package"""
        streams = {}
        if not verbose:
            streams = {
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            }
        project_root = self._find_project_root()
        is_non_released_version = "+" in self._version
        if (project_root / ".git" / "config").exists() and is_non_released_version:
            tmp_dir_path = Path(tmp_dir) / "working-copy"
            # copy everything to a temporary directory
            shutil.copytree(project_root, tmp_dir_path)
            # and override the version file
            version_file = tmp_dir_path / "src/databricks/labs/ucx/__about__.py"
            with version_file.open("w") as f:
                f.write(f'__version__ = "{self._version}"')
            # working copy becomes project root for building a wheel
            project_root = tmp_dir_path
        logger.debug(f"Building wheel for {project_root} in {tmp_dir}")
        subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", tmp_dir, project_root],
            **streams,
            check=True,
        )
        # get wheel name as first file in the temp directory
        return next(Path(tmp_dir).glob("*.whl"))

    def _find_project_root(self) -> Path:
        for leaf in ["pyproject.toml", "setup.py"]:
            root = WorkspaceInstaller._find_dir_with_leaf(self._this_file, leaf)
            if root is not None:
                return root
        msg = "Cannot find project root"
        raise NotADirectoryError(msg)

    @staticmethod
    def _find_dir_with_leaf(folder: Path, leaf: str) -> Path | None:
        root = folder.root
        while str(folder.absolute()) != root:
            if (folder / leaf).exists():
                return folder
            folder = folder.parent
        return None

    def _cluster_node_type(self, spec: compute.ClusterSpec) -> compute.ClusterSpec:
        cfg = self._current_config
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

    def _deployed_steps(self):
        deployed_steps = {}
        logger.debug(f"Fetching all jobs to determine already deployed steps for app={self._app}")
        for j in self._ws.jobs.list():
            tags = j.settings.tags
            if tags is None:
                continue
            if tags.get(TAG_APP, None) != self._app:
                continue
            deployed_steps[tags.get(TAG_STEP, "_")] = j.job_id
        return deployed_steps

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


if __name__ == "__main__":
    ws = WorkspaceClient(product="ucx", product_version=__version__)
    logger.setLevel("INFO")
    installer = WorkspaceInstaller(ws)
    installer.run()

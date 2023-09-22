import logging
import os
import re
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
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.sql import EndpointInfoWarehouseType, SpotInstancePolicy
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import GroupsConfig, MigrationConfig
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.framework.tasks import _TASKS, Task
from databricks.labs.ucx.runtime import main

TAG_STEP = "step"
TAG_APP = "App"

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
from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.framework import logger
from databricks.sdk import WorkspaceClient

logger._install()
logging.getLogger("databricks").setLevel("DEBUG")

cfg = MigrationConfig.from_file(Path("/Workspace{config_file}"))
ws = WorkspaceClient()

print(__version__)
"""

logger = logging.getLogger(__name__)


class Installer:
    def __init__(self, ws: WorkspaceClient, *, prefix: str = "ucx", promtps: bool = True):
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            msg = "Installer is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)
        self._ws = ws
        self._prefix = prefix
        self._prompts = promtps
        self._this_file = Path(__file__)
        self._dashboards = {}

    def run(self):
        logger.info(f"Installing UCX v{__version__}")
        self._configure()
        self._create_dashboards()
        self._create_jobs()

    def _create_dashboards(self):
        local_query_files = self._find_project_root() / "src/databricks/labs/ucx/assessment/queries"
        dash = DashboardFromFiles(
            self._ws,
            local_folder=local_query_files,
            remote_folder=f"{self._install_folder}/queries",
            name=self._name("UCX Assessment"),
            warehouse_id=self._warehouse_id,
            query_text_callback=self._replace_inventory_variable,
        )
        self._dashboards["assessment"] = dash.create_dashboard()

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
            self._config = MigrationConfig.from_bytes(f.read())
        return self._config

    def _name(self, name: str) -> str:
        return f"[{self._prefix.upper()}][{self._short_name}] {name}"

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
        while True:
            inventory_database = self._question("Inventory Database stored in hive_metastore", default="ucx")
            if re.match(r'^\w+$', inventory_database):
                break
            else:
                print(f"{inventory_database} is not a valid database name")

        pro_warehouses = {"[Create new PRO SQL warehouse]": "create_new"} | {
            f"{_.name} ({_.id}, {_.warehouse_type.value}, {_.state.value})": _.id
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
            "Comma-separated list of workspace group names to migrate (empty means all)", default="<ALL>"
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
        self._config = MigrationConfig(
            inventory_database=inventory_database,
            groups=GroupsConfig(**groups_config_args),
            warehouse_id=warehouse_id,
            log_level=log_level,
            num_threads=num_threads,
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
        for step_name in desired_steps:
            settings = self._job_settings(step_name, remote_wheel)
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

    def _create_readme(self):
        md = [
            "# UCX - The Unity Catalog Migration Assistant",
            f'To troubleshoot, see [debug notebook]({self._notebook_link(f"{self._install_folder}/DEBUG.py")}).\n',
            "Here are the URL and descriptions of jobs that trigger's various stages of migration.",
            "All jobs are defined with necessary cluster configurations and DBR versions.",
        ]
        for step_name, job_id in self._deployed_steps.items():
            dashboard_link = ""
            if step_name in self._dashboards:
                dashboard_link = f"{self._ws.config.host}/sql/dashboards/{self._dashboards[step_name]}"
                dashboard_link = f" (see [{step_name} dashboard]({dashboard_link}) after finish)"
            job_link = f"[{self._name(step_name)}]({self._ws.config.host}#job/{job_id})"
            md.append(f"## {job_link}{dashboard_link}\n")
            for t in _TASKS.values():
                if t.workflow != step_name:
                    continue
                doc = re.sub(r"\s+", " ", t.doc)
                doc = self._replace_inventory_variable(doc)
                md.append(f" - `{t.name}`:  {doc}")
                md.append("")
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
        choices = sorted(choices)
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
        if "@" in self._my_username:
            email_notifications = jobs.JobEmailNotifications(
                on_success=[self._my_username], on_failure=[self._my_username]
            )
        tasks = sorted([t for t in _TASKS.values() if t.workflow == step_name], key=lambda _: _.name)
        return {
            "name": self._name(step_name),
            "tags": {TAG_APP: self._app, TAG_STEP: step_name},
            "job_clusters": self._job_clusters({t.job_cluster for t in tasks}),
            "email_notifications": email_notifications,
            "tasks": [self._job_task(task, dbfs_path) for task in tasks],
        }

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
                base_parameters={"inventory_database": self._current_config.inventory_database},
            ),
        )

    def _job_wheel_task(self, jobs_task: jobs.Task, task: Task, dbfs_path: str) -> jobs.Task:
        return replace(
            jobs_task,
            libraries=[compute.Library(whl=f"dbfs:{dbfs_path}")],
            python_wheel_task=jobs.PythonWheelTask(
                package_name="databricks_labs_ucx",
                entry_point="runtime",  # [project.entry-points.databricks] in pyproject.toml
                named_parameters={"task": task.name, "config": f"/Workspace{self._config_file}"},
            ),
        )

    def _job_clusters(self, names: set[str]):
        clusters = []
        spec = self._cluster_node_type(
            compute.ClusterSpec(
                spark_version=self._ws.clusters.select_spark_version(latest=True),
                data_security_mode=compute.DataSecurityMode.NONE,
                spark_conf={"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"},
                custom_tags={"ResourceClass": "SingleNode"},
                num_workers=0,
            )
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

    def _build_wheel(self, tmp_dir: str, *, verbose: bool = False):
        """Helper to build the wheel package"""
        streams = {}
        if not verbose:
            streams = {
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            }
        project_root = self._find_project_root()
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
            root = Installer._find_dir_with_leaf(self._this_file, leaf)
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


if __name__ == "__main__":
    ws = WorkspaceClient(product="ucx", product_version=__version__)
    logger.setLevel("INFO")
    installer = Installer(ws)
    installer.run()

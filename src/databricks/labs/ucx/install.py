import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import webbrowser
from dataclasses import dataclass, replace
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.sql import WidgetOptions
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import GroupsConfig, MigrationConfig, TaclConfig
from databricks.labs.ucx.framework.tasks import _TASKS, Task
from databricks.labs.ucx.mixins import redash
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
from databricks.labs.ucx import logger
from databricks.sdk import WorkspaceClient

logger._install()
logging.getLogger("databricks").setLevel("DEBUG")

cfg = MigrationConfig.from_file(Path("/Workspace{config_file}"))
ws = WorkspaceClient()

print(__version__)
"""

logger = logging.getLogger(__name__)


@dataclass
class SimpleQuery:
    name: str
    query: str
    viz: dict[str, str]
    widget: dict[str, str]

    @property
    def query_key(self):
        return f"{self.name}:query_id"

    @property
    def viz_key(self):
        return f"{self.name}:viz_id"

    @property
    def widget_key(self):
        return f"{self.name}:widget_id"

    @property
    def viz_type(self) -> str:
        return self.viz.get("type", None)

    @property
    def viz_args(self) -> dict:
        return {k: v for k, v in self.viz.items() if k not in ["type"]}


class Installer:
    def __init__(self, ws: WorkspaceClient, *, prefix: str = "ucx", promtps: bool = True):
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            msg = "Installer is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)
        self._ws = ws
        self._prefix = prefix
        self._prompts = promtps
        self._this_file = Path(__file__)

    def run(self):
        self._configure()
        self._create_jobs()

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
        inventory_database = self._question("Inventory Database", default="ucx")
        selected_groups = self._question(
            "Comma-separated list of workspace group names to migrate (empty means all)", default="<ALL>"
        )
        backup_group_prefix = self._question("Backup prefix", default="db-temp-")
        log_level = self._question("Log level", default="INFO")
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
            tacl=TaclConfig(auto=True),
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

    @property
    def _query_dir(self):
        return f"{self._install_folder}/queries"

    @property
    def _query_state(self):
        return f"{self._query_dir}/state.json"

    def _create_dashboards(self):
        desired_queries = self._desired_queries()
        parent, state = self._deployed_query_state()
        data_source_id = self._dashboard_data_source()
        self._deploy_dashboard(state)
        for query in desired_queries:
            self._install_query(query, state, data_source_id, parent)
            self._install_viz(query, state)

            widget_options = WidgetOptions()
            if query.widget_key in state:
                pass
            else:
                widget = self._ws.dashboard_widgets.create(
                    state["dashboard_id"], widget_options, visualization_id=state[query.viz_key]
                )
                state[query.widget_key] = widget.id

        self._store_query_state(desired_queries, state)

        webbrowser.open(self._notebook_link(self._install_folder))

    def _store_query_state(self, desired_queries, state):
        desired_keys = ["dashboard_id"]
        for query in desired_queries:
            desired_keys.append(query.query_key)
            desired_keys.append(query.viz_key)
            desired_keys.append(query.widget_key)
        destructors = {
            "query_id": self._ws.queries.delete,
            "viz_id": self._ws.query_visualizations.delete,
            "widget_id": self._ws.dashboard_widgets.delete,
        }
        new_state = {}
        for k, v in state.items():
            if k in desired_keys:
                new_state[k] = v
                continue
            _, name = k.split(":")
            if name not in destructors:
                continue
            destructors[name](v)
        self._ws.workspace.upload(self._query_state, json.dumps(new_state).encode("utf8"))

    def _deploy_dashboard(self, state):
        if "dashboard_id" in state:
            return
        from databricks.sdk.service.sql import (
            AccessControl,
            ObjectTypePlural,
            PermissionLevel,
            RunAsRole,
        )

        dash = self._ws.dashboards.create(f"[{self._prefix}] Assessment", run_as_role=RunAsRole.VIEWER)
        self._ws.dbsql_permissions.set(
            ObjectTypePlural.DASHBOARDS,
            dash.id,
            access_control_list=[AccessControl(group_name="users", permission_level=PermissionLevel.CAN_VIEW)],
        )
        state["dashboard_id"] = dash.id

    def _deployed_query_state(self):
        state = {}
        try:
            state = json.load(self._ws.workspace.download(self._query_state))
        except DatabricksError as err:
            if err.error_code != "RESOURCE_DOES_NOT_EXIST":
                raise err
            self._ws.workspace.mkdirs(self._query_dir)
        object_info = self._ws.workspace.get_status(self._query_dir)
        parent = f"folders/{object_info.object_id}"
        return parent, state

    def _desired_queries(self) -> list[SimpleQuery]:
        desired_queries = []
        local_query_files = self._find_project_root() / "src/databricks/labs/ucx/assessment/queries"
        for f in local_query_files.glob("*.sql"):
            text = f.read_text("utf8")
            text = text.replace("$inventory", f"hive_metastore.{self._current_config.inventory_database}")
            desired_queries.append(
                SimpleQuery(
                    name=f.name,
                    query=text,
                    viz=self._parse_magic_comment(f, "-- viz ", text),
                    widget=self._parse_magic_comment(f, "-- widget ", text),
                )
            )
        return desired_queries

    def _install_viz(self, query, state):
        viz_types = {"table": self._table_viz_args}
        if query.viz_type not in viz_types:
            msg = f"{query.query}: unknown viz type: {query.viz_type}"
            raise SyntaxError(msg)

        viz_args = viz_types[query.viz_type](**query.viz_args)
        if query.viz_key in state:
            return self._ws.query_visualizations.update(state[query.viz_key], **viz_args)
        viz = self._ws.query_visualizations.create(state[query.query_key], **viz_args)
        state[query.viz_key] = viz.id

    def _install_query(self, query: SimpleQuery, state: dict, data_source_id: str, parent: str):
        from databricks.sdk.service.sql import (
            AccessControl,
            ObjectTypePlural,
            PermissionLevel,
            RunAsRole,
        )

        query_meta = {"data_source_id": data_source_id, "name": f"[{self._prefix}] {query.name}", "query": query.query}
        if query.query_key in state:
            return self._ws.queries.update(state[query.query_key], **query_meta)

        deployed_query = self._ws.queries.create(parent=parent, run_as_role=RunAsRole.VIEWER, **query_meta)
        self._ws.dbsql_permissions.set(
            ObjectTypePlural.QUERIES,
            deployed_query.id,
            access_control_list=[AccessControl(group_name="users", permission_level=PermissionLevel.CAN_RUN)],
        )
        state[query.query_key] = deployed_query.id

    def _table_viz_args(
        self,
        name: str,
        columns: str,
        *,
        items_per_page: int = 25,
        condensed=True,
        with_row_number=False,
        description: str | None = None,
    ) -> dict:
        return {
            "type": "TABLE",
            "name": name,
            "description": description,
            "options": {
                "itemsPerPage": items_per_page,
                "condensed": condensed,
                "withRowNumber": with_row_number,
                "version": 2,
                "columns": [redash.VizColumn(name=x, title=x).as_dict() for x in columns.split(",")],
            },
        }

    def _parse_magic_comment(self, f, magic_comment, text):
        viz_comment = next(l for l in text.splitlines() if l.startswith(magic_comment))
        if not viz_comment:
            msg = f'{f}: cannot find "{magic_comment}" magic comment'
            raise SyntaxError(msg)
        return dict(_.split("=") for _ in viz_comment.replace(magic_comment, "").split(", "))

    def _dashboard_data_source(self) -> str:
        data_sources = {_.warehouse_id: _.id for _ in self._ws.data_sources.list()}
        warehouses = self._ws.warehouses.list()
        warehouse_id = self._current_config.warehouse_id
        if not warehouse_id and not warehouses:
            msg = "need either configured warehouse_id or an existing SQL warehouse"
            raise ValueError(msg)
        if not warehouse_id:
            warehouse_id = warehouses[0].id
        data_source_id = data_sources[warehouse_id]
        return data_source_id

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
            md.append(f"## [[{self._prefix.upper()}] {step_name}]({self._ws.config.host}#job/{job_id})\n")
            for t in _TASKS.values():
                if t.workflow != step_name:
                    continue
                doc = re.sub(r"\s+", " ", t.doc)
                md.append(f" - `{t.name}`:  {doc}")
                md.append("")
        preamble = ["# Databricks notebook source", "# MAGIC %md"]
        intro = "\n".join(preamble + [f"# MAGIC {line}" for line in md])
        path = f"{self._install_folder}/README.py"
        self._ws.workspace.upload(path, intro.encode("utf8"), overwrite=True)
        url = self._notebook_link(path)
        logger.info(f"Created README notebook with job overview: {url}")
        msg = "Open job overview in README notebook in your home directory"
        if self._prompts and self._question(msg, default="yes") == "yes":
            webbrowser.open(url)

    def _create_debug(self, remote_wheel: str):
        readme_link = self._notebook_link(f"{self._install_folder}/README.py")
        job_links = ", ".join(
            f"[[{self._prefix.upper()}] {step_name}]({self._ws.config.host}#job/{job_id})"
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
            "name": f"[{self._prefix.upper()}] {step_name}",
            "tags": {TAG_APP: self._prefix, TAG_STEP: step_name},
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
        if task.notebook:
            return self._job_notebook_task(jobs_task, task)
        return self._job_wheel_task(jobs_task, task, dbfs_path)

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
        logger.debug(f"Fetching all jobs to determine already deployed steps for app={self._prefix}")
        for j in self._ws.jobs.list():
            tags = j.settings.tags
            if tags is None:
                continue
            if tags.get(TAG_APP, None) != self._prefix:
                continue
            deployed_steps[tags.get(TAG_STEP, "_")] = j.job_id
        return deployed_steps


if __name__ == "__main__":
    ws = WorkspaceClient(product="ucx", product_version=__version__)
    installer = Installer(ws)
    installer.run()

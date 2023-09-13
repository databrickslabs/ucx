import logging
import os
import re
import subprocess
import sys
import tempfile
import webbrowser
from dataclasses import replace
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import GroupsConfig, MigrationConfig, TaclConfig
from databricks.labs.ucx.runtime import main
from databricks.labs.ucx.tasks import _TASKS

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

from databricks.labs.ucx.__about__ import __version__
print(f'Debugging UCX v{__version__}')
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
        ws_file_url = f"{self._ws.config.host}/#workspace{self._config_file}"
        try:
            self._ws.workspace.get_status(self._config_file)
            logger.info(f"UCX is already configured. See {ws_file_url}")
            if self._prompts and self._question("Type 'yes' to open config file in the browser") == "yes":
                webbrowser.open(ws_file_url)
            return
        except DatabricksError as err:
            if err.error_code != "RESOURCE_DOES_NOT_EXIST":
                raise err

        logger.info("Please answer a couple of questions to configure Unity Catalog migration")
        self._config = MigrationConfig(
            inventory_database=self._question("Inventory Database", default="ucx"),
            groups=GroupsConfig(
                selected=self._question("Comma-separated list of workspace group names to migrate").split(","),
                backup_group_prefix=self._question("Backup prefix", default="db-temp-"),
            ),
            tacl=TaclConfig(auto=True),
            log_level=self._question("Log level", default="INFO"),
            num_threads=int(self._question("Number of threads", default="8")),
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
            "Here are the descriptions of jobs that trigger various stages of migration.",
            f'To troubleshoot, see [debug notebook]({self._notebook_link(f"{self._install_folder}/DEBUG.py")}).',
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
        msg = "Type 'yes' to open job overview in README notebook in your home directory"
        if self._prompts and self._question(msg) == "yes":
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
            DEBUG_NOTEBOOK.format(remote_wheel=remote_wheel, readme_link=readme_link, job_links=job_links).encode(
                "utf8"
            ),
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
            with local_wheel.open("rb") as f:
                logger.info(f"Uploading wheel to dbfs:{remote_wheel}")
                self._ws.dbfs.upload(remote_wheel, f, overwrite=True)
            with local_wheel.open("rb") as f:
                logger.info(f"Uploading wheel to /Workspace{remote_wheel}")
                self._ws.workspace.upload(remote_wheel, f, overwrite=True, format=ImportFormat.AUTO)
        return remote_wheel

    def _job_settings(self, step_name, dbfs_path):
        config_file = f"/Workspace/{self._install_folder}/config.yml"
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
            "tasks": [
                jobs.Task(
                    task_key=task.name,
                    job_cluster_key=task.job_cluster,
                    depends_on=[jobs.TaskDependency(task_key=d) for d in _TASKS[task.name].depends_on],
                    libraries=[compute.Library(whl=f"dbfs:{dbfs_path}")],
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="databricks_labs_ucx",
                        entry_point="runtime",  # [project.entry-points.databricks] in pyproject.toml
                        named_parameters={"task": task.name, "config": config_file},
                    ),
                )
                for task in tasks
            ],
        }

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
                        custom_tags={},
                    ),
                )
            )
        return clusters

    @staticmethod
    def _build_wheel(tmp_dir: str, *, verbose: bool = False):
        """Helper to build the wheel package"""
        streams = {}
        if not verbose:
            streams = {
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            }
        project_root = Installer._find_project_root(Path(__file__))
        if not project_root:
            msg = "Cannot find project root"
            raise NotADirectoryError(msg)
        logger.debug(f"Building wheel for {project_root} in {tmp_dir}")
        subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", tmp_dir, project_root],
            **streams,
            check=True,
        )
        # get wheel name as first file in the temp directory
        return next(Path(tmp_dir).glob("*.whl"))

    @staticmethod
    def _find_project_root(folder: Path) -> Path | None:
        for leaf in ["pyproject.toml", "setup.py"]:
            root = Installer._find_dir_with_leaf(folder, leaf)
            if root is not None:
                return root
        return None

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

"""Deployment of the UCX Databricks App: `databricks labs ucx deploy-app`.

The app runs the released UCX wheel, which contains the built frontend and the hashed requirements generated from
uv.lock (see `make build`). A `databricks labs install ucx` comes from the source archive, which has neither, so the
wheel of the installed version is downloaded from the package index and verified against the index's sha256 digest.
"""

import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from string import Template
from urllib.parse import urljoin

import requests
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.app.auth import AuthMode
from databricks.labs.ucx.config import WorkspaceConfig

logger = logging.getLogger(__name__)

DISTRIBUTION = "databricks-labs-ucx"
DEFAULT_INDEX_URL = "https://pypi.org/simple"
BUNDLE_PREFIX = "databricks/labs/ucx/app/bundle/"
FRONTEND_INDEX = "databricks/labs/ucx/app/static/index.html"
APP_RESOURCE_KEY = "ucx_app"

BundleRunner = Callable[[list[str], Path], None]


class AppDeploymentError(RuntimeError):
    """The app could not be deployed."""


def wheel_filename(version: str) -> str:
    return f"{DISTRIBUTION.replace('-', '_')}-{version}-py3-none-any.whl"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_released_wheel(version: str, target_dir: Path, *, index_url: str | None = None) -> Path:
    """Download the released UCX wheel of a version and verify it against the digest published by the index.

    Uses the JSON form of the simple repository API (PEP 691), on PIP_INDEX_URL when set, so that package proxies work.
    """
    index_url = index_url or os.environ.get("PIP_INDEX_URL") or DEFAULT_INDEX_URL
    project_url = f"{index_url.rstrip('/')}/{DISTRIBUTION}/"
    response = requests.get(project_url, headers={"Accept": "application/vnd.pypi.simple.v1+json"}, timeout=60)
    response.raise_for_status()
    filename = wheel_filename(version)
    files = [f for f in response.json().get("files", []) if f.get("filename") == filename]
    if not files:
        raise AppDeploymentError(f"{filename} not found on the package index. Pass a locally built wheel with --wheel.")
    expected = files[0].get("hashes", {}).get("sha256")
    if not expected:
        raise AppDeploymentError(f"The package index publishes no sha256 digest for {filename}")
    path = target_dir / filename
    with requests.get(urljoin(project_url, files[0]["url"]), stream=True, timeout=300) as download:
        download.raise_for_status()
        with path.open("wb") as f:
            for chunk in download.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    actual = sha256(path)
    if actual != expected:
        path.unlink()
        raise AppDeploymentError(f"sha256 of {filename} is {actual}, but the package index publishes {expected}")
    logger.info(f"Downloaded {filename} (sha256 {actual})")
    return path


def run_bundle_command(args: list[str], cwd: Path) -> None:
    """Run a Databricks CLI command. Authentication is inherited from the environment, like the labs CLI sets it."""
    cli = shutil.which("databricks")
    if not cli:
        raise AppDeploymentError("Databricks CLI not found on PATH: it's needed to deploy the app bundle")
    logger.info(f"Running: databricks {' '.join(args)}")
    result = subprocess.run([cli, *args], cwd=cwd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise AppDeploymentError(f"databricks {' '.join(args[:2])} failed:\n{result.stderr or result.stdout}")


@dataclass
class DeployedApp:
    name: str
    url: str
    service_principal: str


class AppDeployer:
    def __init__(
        self,
        ws: WorkspaceClient,
        installation: Installation,
        config: WorkspaceConfig,
        install_state: InstallState,
        *,
        run_bundle: BundleRunner = run_bundle_command,
        download_wheel: Callable[[str, Path], Path] = download_released_wheel,
    ):
        self._ws = ws
        self._installation = installation
        self._config = config
        self._install_state = install_state
        self._run_bundle = run_bundle
        self._download_wheel = download_wheel

    def deploy(self, *, app_name: str, run_as_sp: bool, wheel: Path | None = None) -> DeployedApp:
        auth_mode = AuthMode.SP if run_as_sp else AuthMode.OBO
        with tempfile.TemporaryDirectory(prefix="ucx-app-") as tmp:
            staging = Path(tmp)
            if wheel is None:
                if "+" in __version__ or "dev" in __version__:
                    raise AppDeploymentError(f"UCX {__version__} is not a release: pass a wheel with --wheel")
                wheel = self._download_wheel(__version__, staging)
            self._stage(staging, wheel, auth_mode)
            variables = [
                f"--var=app_name={app_name}",
                f"--var=warehouse_id={self._config.warehouse_id}",
                f"--var=bundle_root={self._installation.install_folder()}/app",
            ]
            self._run_bundle(["bundle", "deploy", *variables], staging)
            self._run_bundle(["bundle", "run", APP_RESOURCE_KEY, *variables], staging)
        app = self._ws.apps.get(app_name)
        service_principal = app.service_principal_client_id
        if not service_principal:
            raise AppDeploymentError(f"App {app_name} has no service principal")
        self._grant_job_runs(service_principal)
        if auth_mode == AuthMode.SP:
            self._grant_install_folder(service_principal)
            logger.warning(
                f"The app runs as service principal {service_principal}: grant it read access to the inventory "
                f"schema hive_metastore.{self._config.inventory_database}, and to the tables it migrates."
            )
        return DeployedApp(name=app_name, url=app.url or "", service_principal=service_principal)

    def _stage(self, staging: Path, wheel: Path, auth_mode: AuthMode) -> None:
        """Lay out the bundle: databricks.yml, and app/ with app.yaml, requirements.txt and the wheel.

        The bundle files come from the wheel, so the templates always match the code that runs.
        """
        with zipfile.ZipFile(wheel) as archive:
            names = set(archive.namelist())
            missing = {FRONTEND_INDEX, f"{BUNDLE_PREFIX}requirements.txt"} - names
            if missing:
                raise AppDeploymentError(
                    f"{wheel.name} doesn't contain the app ({', '.join(sorted(missing))}): build it with `make build`"
                )

            def read(name: str) -> str:
                return archive.read(f"{BUNDLE_PREFIX}{name}").decode("utf-8")

            app_dir = staging / "app"
            (app_dir / "wheels").mkdir(parents=True)
            (staging / "databricks.yml").write_text(read("databricks.yml"))
            install_folder = self._installation.install_folder() if auth_mode == AuthMode.SP else ""
            app_yaml = Template(read("app.yaml")).substitute(auth_mode=auth_mode.value, install_folder=install_folder)
            (app_dir / "app.yaml").write_text(app_yaml)
            shutil.copy(wheel, app_dir / "wheels" / wheel.name)
            # Every requirement has a hash, so pip runs in hash-checking mode: the wheel needs one too.
            requirements = read("requirements.txt").rstrip("\n")
            requirements += f"\n./wheels/{wheel.name} \\\n    --hash=sha256:{sha256(wheel)}\n"
            (app_dir / "requirements.txt").write_text(requirements)

    def _grant_job_runs(self, service_principal: str) -> None:
        # User authorization has no Jobs API scope: the app always starts and watches workflows as its SP.
        for workflow, job_id in self._install_state.jobs.items():
            logger.info(f"Granting CAN_MANAGE_RUN on {workflow} to the app")
            self._ws.permissions.update(
                "jobs",
                str(job_id),
                access_control_list=[
                    AccessControlRequest(
                        service_principal_name=service_principal, permission_level=PermissionLevel.CAN_MANAGE_RUN
                    )
                ],
            )

    def _grant_install_folder(self, service_principal: str) -> None:
        install_folder = self._installation.install_folder()
        status = self._ws.workspace.get_status(install_folder)
        logger.info(f"Granting CAN_EDIT on {install_folder} to the app")
        self._ws.permissions.update(
            "directories",
            str(status.object_id),
            access_control_list=[
                AccessControlRequest(
                    service_principal_name=service_principal, permission_level=PermissionLevel.CAN_EDIT
                )
            ],
        )

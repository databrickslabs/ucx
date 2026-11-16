import hashlib
import zipfile
from pathlib import Path
from unittest.mock import create_autospec, patch

import pytest
import requests
import yaml
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.workspace import ObjectInfo

from databricks.labs.ucx.app import deploy
from databricks.labs.ucx.app.deploy import (
    AppDeployer,
    AppDeploymentError,
    download_released_wheel,
    wheel_filename,
)
from databricks.labs.ucx.cli import deploy_app
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext

BUNDLE_SOURCE = Path(deploy.__file__).parent / "bundle"
REQUIREMENTS = "fastapi==0.135.4 \\\n    --hash=sha256:abc\n"


def make_wheel(path: Path, *, with_frontend: bool = True) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name in ("databricks.yml", "app.yaml"):
            archive.write(BUNDLE_SOURCE / name, f"databricks/labs/ucx/app/bundle/{name}")
        archive.writestr("databricks/labs/ucx/app/bundle/requirements.txt", REQUIREMENTS)
        if with_frontend:
            archive.writestr("databricks/labs/ucx/app/static/index.html", "<html></html>")
    return path


@pytest.fixture
def installation() -> MockInstallation:
    return MockInstallation(
        {
            "config.yml": {"inventory_database": "ucx", "warehouse_id": "wh-1", "connect": {"host": "a", "token": "b"}},
            "state.json": {"resources": {"jobs": {"assessment": "1", "migrate-tables": "2"}}},
        }
    )


@pytest.fixture
def apps_ws() -> WorkspaceClient:
    ws = create_autospec(WorkspaceClient)
    ws.apps.get.return_value = App(
        name="ucx", url="https://ucx-123.databricksapps.com", service_principal_client_id="sp-1"
    )
    ws.workspace.get_status.return_value = ObjectInfo(object_id=987)
    return ws


class StagingRecorder:
    """Records the bundle commands, and the staged files at the time of the first command."""

    def __init__(self):
        self.commands: list[list[str]] = []
        self.files: dict[str, str] = {}

    def __call__(self, args: list[str], cwd: Path) -> None:
        self.commands.append(args)
        if not self.files:
            for path in cwd.rglob("*"):
                if path.is_file():
                    content = path.read_bytes()
                    rel = str(path.relative_to(cwd))
                    self.files[rel] = content.decode("utf-8") if not rel.endswith(".whl") else "<wheel>"


def make_deployer(workspace, install, recorder, **kwargs) -> AppDeployer:
    config = install.load(WorkspaceConfig)
    return AppDeployer(
        workspace, install, config, InstallState.from_installation(install), run_bundle=recorder, **kwargs
    )


def test_deploy_stages_bundle_and_runs_it(apps_ws, installation, tmp_path) -> None:
    wheel = make_wheel(tmp_path / wheel_filename("0.61.0"))
    recorder = StagingRecorder()

    app = make_deployer(apps_ws, installation, recorder).deploy(app_name="my-ucx", run_as_sp=False, wheel=wheel)

    assert app.url == "https://ucx-123.databricksapps.com"
    variables = [
        "--var=app_name=my-ucx",
        "--var=warehouse_id=wh-1",
        f"--var=bundle_root={installation.install_folder()}/app",
    ]
    assert recorder.commands == [["bundle", "deploy", *variables], ["bundle", "run", "ucx_app", *variables]]
    assert set(recorder.files) == {
        "databricks.yml",
        "app/app.yaml",
        "app/requirements.txt",
        f"app/wheels/{wheel.name}",
    }
    env = {e["name"]: e["value"] for e in yaml.safe_load(recorder.files["app/app.yaml"])["env"]}
    assert env == {"UCX_APP_AUTH_MODE": "obo", "UCX_INSTALL_FOLDER": ""}
    wheel_hash = hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert recorder.files["app/requirements.txt"] == (
        f"{REQUIREMENTS}./wheels/{wheel.name} \\\n    --hash=sha256:{wheel_hash}\n"
    )
    bundle = yaml.safe_load(recorder.files["databricks.yml"])
    assert bundle["resources"]["apps"]["ucx_app"]["source_code_path"] == "./app"


def test_deploy_grants_job_runs_to_app(apps_ws, installation, tmp_path) -> None:
    wheel = make_wheel(tmp_path / wheel_filename("0.61.0"))

    make_deployer(apps_ws, installation, StagingRecorder()).deploy(app_name="ucx", run_as_sp=False, wheel=wheel)

    grant = [AccessControlRequest(service_principal_name="sp-1", permission_level=PermissionLevel.CAN_MANAGE_RUN)]
    assert [c.args for c in apps_ws.permissions.update.call_args_list] == [("jobs", "1"), ("jobs", "2")]
    assert all(c.kwargs == {"access_control_list": grant} for c in apps_ws.permissions.update.call_args_list)


def test_deploy_as_service_principal(apps_ws, installation, tmp_path) -> None:
    wheel = make_wheel(tmp_path / wheel_filename("0.61.0"))
    recorder = StagingRecorder()

    make_deployer(apps_ws, installation, recorder).deploy(app_name="ucx", run_as_sp=True, wheel=wheel)

    env = {e["name"]: e["value"] for e in yaml.safe_load(recorder.files["app/app.yaml"])["env"]}
    assert env == {"UCX_APP_AUTH_MODE": "sp", "UCX_INSTALL_FOLDER": installation.install_folder()}
    apps_ws.permissions.update.assert_any_call(
        "directories",
        "987",
        access_control_list=[
            AccessControlRequest(service_principal_name="sp-1", permission_level=PermissionLevel.CAN_EDIT)
        ],
    )


def test_deploy_rejects_wheel_without_frontend(apps_ws, installation, tmp_path) -> None:
    wheel = make_wheel(tmp_path / wheel_filename("0.61.0"), with_frontend=False)
    recorder = StagingRecorder()

    with pytest.raises(AppDeploymentError, match="doesn't contain the app"):
        make_deployer(apps_ws, installation, recorder).deploy(app_name="ucx", run_as_sp=False, wheel=wheel)
    assert not recorder.commands


def test_deploy_downloads_released_wheel(apps_ws, installation, tmp_path) -> None:
    def download(version: str, target: Path) -> Path:
        assert version == "0.61.0"
        return make_wheel(target / wheel_filename(version))

    with patch.object(deploy, "__version__", "0.61.0"):
        app = make_deployer(apps_ws, installation, StagingRecorder(), download_wheel=download).deploy(
            app_name="ucx", run_as_sp=False
        )

    assert app.service_principal == "sp-1"


def test_deploy_dev_version_requires_wheel(apps_ws, installation) -> None:
    with patch.object(deploy, "__version__", "0.61.0+1220251116"), pytest.raises(AppDeploymentError, match="--wheel"):
        make_deployer(apps_ws, installation, StagingRecorder()).deploy(app_name="ucx", run_as_sp=False)


def _index_response(filename: str, digest: str) -> requests.Response:
    response = create_autospec(requests.Response)
    response.json.return_value = {
        "files": [
            {"filename": "databricks_labs_ucx-0.61.0.tar.gz", "url": "x.tar.gz", "hashes": {"sha256": "0"}},
            {"filename": filename, "url": f"../../packages/{filename}", "hashes": {"sha256": digest}},
        ]
    }
    return response


def _download_response(content: bytes) -> requests.Response:
    response = create_autospec(requests.Response)
    response.__enter__.return_value = response
    response.iter_content.return_value = [content]
    return response


def test_download_released_wheel_verifies_digest(tmp_path) -> None:
    content = b"wheel bytes"
    filename = wheel_filename("0.61.0")
    responses = [_index_response(filename, hashlib.sha256(content).hexdigest()), _download_response(content)]

    with patch.object(deploy.requests, "get", side_effect=responses) as get:
        path = download_released_wheel("0.61.0", tmp_path, index_url="https://pypi.example.com/simple")

    assert path.read_bytes() == content
    assert get.call_args_list[0].args == ("https://pypi.example.com/simple/databricks-labs-ucx/",)
    assert get.call_args_list[0].kwargs["headers"] == {"Accept": "application/vnd.pypi.simple.v1+json"}
    assert get.call_args_list[1].args == (f"https://pypi.example.com/packages/{filename}",)


def test_download_released_wheel_rejects_digest_mismatch(tmp_path) -> None:
    filename = wheel_filename("0.61.0")
    responses = [_index_response(filename, "0" * 64), _download_response(b"tampered")]

    with patch.object(deploy.requests, "get", side_effect=responses), pytest.raises(AppDeploymentError, match="sha256"):
        download_released_wheel("0.61.0", tmp_path, index_url="https://pypi.example.com/simple")

    assert not (tmp_path / filename).exists()


def test_download_released_wheel_not_published(tmp_path) -> None:
    response = create_autospec(requests.Response)
    response.json.return_value = {"files": []}

    with (
        patch.object(deploy.requests, "get", return_value=response),
        pytest.raises(AppDeploymentError, match="--wheel"),
    ):
        download_released_wheel("9.9.9", tmp_path, index_url="https://pypi.example.com/simple")


def test_cli_deploy_app(apps_ws, installation, tmp_path) -> None:
    wheel = make_wheel(tmp_path / wheel_filename("0.61.0"))
    ctx = WorkspaceContext(apps_ws).replace(installation=installation)

    with (
        patch.object(deploy.shutil, "which", return_value="/usr/local/bin/databricks"),
        patch.object(deploy.subprocess, "run") as run,
        patch("webbrowser.open") as open_browser,
    ):
        run.return_value.returncode = 0
        deploy_app(apps_ws, app_name="ucx", run_as_sp=False, wheel=str(wheel), ctx=ctx)

    assert [c.args[0][:3] for c in run.call_args_list] == [
        ["/usr/local/bin/databricks", "bundle", "deploy"],
        ["/usr/local/bin/databricks", "bundle", "run"],
    ]
    open_browser.assert_called_once_with("https://ucx-123.databricksapps.com")


def test_bundle_command_failure(tmp_path) -> None:
    with (
        patch.object(deploy.shutil, "which", return_value="/usr/local/bin/databricks"),
        patch.object(deploy.subprocess, "run") as run,
    ):
        run.return_value.returncode = 1
        run.return_value.stderr = "Error: app name already in use"
        with pytest.raises(AppDeploymentError, match="app name already in use"):
            deploy.run_bundle_command(["bundle", "deploy"], tmp_path)


def test_bundle_command_requires_cli(tmp_path) -> None:
    with patch.object(deploy.shutil, "which", return_value=None), pytest.raises(AppDeploymentError, match="CLI"):
        deploy.run_bundle_command(["bundle", "deploy"], tmp_path)

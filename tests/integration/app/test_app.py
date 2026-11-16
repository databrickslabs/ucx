import logging
from datetime import timedelta
from pathlib import Path
from unittest.mock import create_autospec

import requests
from fastapi.testclient import TestClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.jobs import ConditionTask, ConditionTaskOp, Run, RunResultState, Task
from databricks.sdk.service.provisioning import Workspace

from databricks.labs.ucx.app.context import AppContext
from databricks.labs.ucx.app.deploy import AppDeployer
from databricks.labs.ucx.app.main import create_app
from databricks.labs.ucx.app.routes.dependencies import get_service
from databricks.labs.ucx.app.service import (
    MIGRATE_CTAS_WORKFLOW,
    MIGRATE_TABLES_WORKFLOW,
    UcxAppService,
)
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.progress.install import VerifyProgressTracking

logger = logging.getLogger(__name__)


def app_client(ctx: AppContext) -> TestClient:
    """The app's API, in-process, against the real workspace."""
    app = create_app()
    app.dependency_overrides[get_service] = lambda: UcxAppService(ctx, "obo")
    return TestClient(app)


def app_context(ws: WorkspaceClient, installation_ctx, *, with_inventory: bool = True) -> AppContext:
    """The app's context on the test installation. The inventory needs the config, which starts the test clusters."""
    ctx = AppContext(ws).replace(
        installation=installation_ctx.installation,
        install_state=installation_ctx.install_state,
    )
    if with_inventory:
        ctx.replace(
            config=installation_ctx.config,
            sql_backend=installation_ctx.sql_backend,
            tables_crawler=installation_ctx.tables_crawler,
        )
    return ctx


def test_mapping_roundtrip(ws, installation_ctx) -> None:
    schema = installation_ctx.make_schema(catalog_name="hive_metastore")
    table = installation_ctx.make_table(schema_name=schema.name)
    installation_ctx.with_workspace_info([Workspace(workspace_id=ws.get_workspace_id(), workspace_name="App Test")])
    client = app_client(app_context(ws, installation_ctx))

    created = client.post("/api/mapping")

    assert created.status_code == 200, created.text
    rules = created.json()["rules"]
    assert [(r["src_schema"], r["src_table"], r["catalog_name"]) for r in rules] == [
        (schema.name, table.name, "App_Test")
    ]

    edited = [{**rules[0], "catalog_name": "app_test_target", "dst_table": f"{table.name}_renamed"}]
    saved = client.put("/api/mapping", json={"rules": edited})

    assert saved.status_code == 200, saved.text
    # Read back from the workspace, the way the migrate-tables workflow reads it.
    on_workspace = installation_ctx.installation.load(list[Rule], filename=TableMapping.FILENAME)
    assert [(r.catalog_name, r.dst_table) for r in on_workspace] == [("app_test_target", f"{table.name}_renamed")]
    assert client.get("/api/mapping").json()["rules"] == edited

    duplicate = client.put("/api/mapping", json={"rules": [edited[0], {**edited[0], "src_table": "other"}]})

    assert duplicate.status_code == 422
    assert len(installation_ctx.installation.load(list[Rule], filename=TableMapping.FILENAME)) == 1


def test_status_dashboard_and_runs(ws, installation_ctx) -> None:
    installation_ctx.workspace_installation.run()
    client = app_client(app_context(ws, installation_ctx))

    status = client.get("/api/status")

    assert status.status_code == 200, status.text
    assert status.json()["user"] == ws.current_user.me().user_name
    assert {"assessment", MIGRATE_TABLES_WORKFLOW} <= {w["step"] for w in status.json()["workflows"]}

    dashboard = client.get("/api/assessment/dashboard")

    assert dashboard.status_code == 200, dashboard.text
    assert ws.lakeview.get(dashboard.json()["dashboard_id"]).display_name

    active = client.get("/api/runs/active")

    assert active.status_code == 200, active.text
    assert active.json() == {"runs": []}


def test_chained_migration_run(ws, installation_ctx, make_job) -> None:
    """The selected migration workflows run one after the other, as `run_job_task`s of a single submitted run.

    The UCX migration workflows are replaced by jobs that need no compute, so that only the chaining is tested.
    """
    tasks = [Task(task_key="noop", condition_task=ConditionTask(op=ConditionTaskOp.EQUAL_TO, left="1", right="1"))]
    first, second = make_job(tasks=tasks), make_job(tasks=tasks)
    installation_ctx.install_state.jobs[MIGRATE_TABLES_WORKFLOW] = str(first.job_id)
    installation_ctx.install_state.jobs[MIGRATE_CTAS_WORKFLOW] = str(second.job_id)
    installation_ctx.install_state.save()
    # The chaining doesn't need a UC catalog or an assessment run: the prerequisites are met.
    verify = create_autospec(VerifyProgressTracking)
    verify.verify.return_value = None
    ctx = app_context(ws, installation_ctx, with_inventory=False).replace(verify_progress_tracking=verify)
    client = app_client(ctx)

    started = client.post("/api/migration/run", json={"migrate_tables": True, "external_ctas": True})

    assert started.status_code == 200, started.text
    run_id = started.json()["run_id"]
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(run_id=run_id, timeout=timedelta(minutes=10))
    assert run.state and run.state.result_state == RunResultState.SUCCESS, _describe_failure(ws, run)
    assert run.run_name and "(requested by " in run.run_name
    tasks_by_key = {task.task_key: task for task in run.tasks or []}
    assert set(tasks_by_key) == {"migrate_tables", "migrate_external_tables_ctas"}
    assert tasks_by_key["migrate_tables"].run_job_task.job_id == first.job_id
    assert tasks_by_key["migrate_external_tables_ctas"].run_job_task.job_id == second.job_id
    # Sequential: the second workflow only started after the first one finished.
    assert tasks_by_key["migrate_external_tables_ctas"].start_time >= tasks_by_key["migrate_tables"].end_time

    finished = client.get(f"/api/runs/{run_id}")

    assert finished.status_code == 200, finished.text
    assert finished.json()["state"] == "SUCCESS"


def _describe_failure(ws: WorkspaceClient, run: Run) -> str:
    """The state of each task of a run, and of the job runs that they started, to explain a failed run."""
    lines = [f"run {run.run_id}: {run.state}"]
    for task in run.tasks or []:
        lines.append(f"task {task.task_key}: {task.state}")
        if not task.run_id:
            continue
        try:
            output = ws.jobs.get_run_output(task.run_id)
            lines.append(f"  output: error={output.error!r} trace={(output.error_trace or '')[:500]!r}")
        except DatabricksError as e:
            lines.append(f"  output not available: {e}")
        child = ws.jobs.get_run(task.run_id)
        for child_task in child.tasks or []:
            lines.append(f"  child task {child_task.task_key}: {child_task.state}")
    return "\n".join(lines)


def test_deploy_app_end_to_end(ws, installation_ctx, make_random, env_or_skip) -> None:
    """Deploys the app with `deploy-app` and calls its API. Needs a wheel built with `make build`."""
    wheel = Path(env_or_skip("UCX_APP_WHEEL"))
    installation_ctx.workspace_installation.run()
    app_name = f"ucx-test-{make_random(6).lower()}"
    deployer = AppDeployer(ws, installation_ctx.installation, installation_ctx.config, installation_ctx.install_state)
    try:
        deployed = deployer.deploy(app_name=app_name, run_as_sp=False, wheel=wheel)
        assert deployed.url

        # The workspace token is accepted by the app proxy, and forwarded to the app as the user's token.
        status = _get_when_ready(f"{deployed.url}/api/status", ws.config.authenticate())
        assert status.json()["user"] == ws.current_user.me().user_name
        assert status.json()["install_folder"] == installation_ctx.installation.install_folder()

        # Jobs go through the app's service principal, which deploy-app granted CAN_MANAGE_RUN.
        active = requests.get(f"{deployed.url}/api/runs/active", headers=ws.config.authenticate(), timeout=60)
        assert active.status_code == 200, active.text
    finally:
        try:
            ws.apps.delete(app_name)
        except NotFound:
            pass


@retried(on=[AssertionError], timeout=timedelta(minutes=5))
def _get_when_ready(url: str, headers: dict[str, str]) -> requests.Response:
    response = requests.get(url, headers=headers, timeout=60)
    assert response.status_code == 200, f"{response.status_code}: {response.text[:300]}"
    return response

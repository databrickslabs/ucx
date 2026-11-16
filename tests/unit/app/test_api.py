from typing import Any
from unittest.mock import create_autospec

import pytest
from fastapi.testclient import TestClient
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk.errors import InvalidParameterValue, NotFound
from databricks.sdk.service.jobs import (
    BaseRun,
    Run,
    RunJobTask,
    RunLifeCycleState,
    RunResultState,
    RunState,
    RunType,
    SubmitTask,
    TaskDependency,
)

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.app.context import AppContext
from databricks.labs.ucx.app.main import create_app
from databricks.labs.ucx.app.routes.dependencies import get_service
from databricks.labs.ucx.app.service import UcxAppService
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.mapping import Rule
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.progress.install import VerifyProgressTracking

CONFIG: dict[str, Any] = {"inventory_database": "ucx", "warehouse_id": "abc", "connect": {"host": "a", "token": "b"}}
RULE: dict[str, Any] = {
    "workspace_name": "workspace",
    "catalog_name": "catalog",
    "src_schema": "schema",
    "dst_schema": "schema",
    "src_table": "table",
    "dst_table": "table",
}


@pytest.fixture
def installation() -> MockInstallation:
    return MockInstallation(
        {
            "config.yml": CONFIG,
            "mapping.csv": [RULE],
            "state.json": {
                "resources": {
                    "jobs": {
                        "assessment": "1",
                        "migrate-tables": "2",
                        "migrate-external-hiveserde-tables-in-place-experimental": "3",
                        "migrate-external-tables-ctas": "4",
                    },
                    "dashboards": {"assessment_main": "dash-1"},
                }
            },
        }
    )


@pytest.fixture
def tables_crawler() -> TablesCrawler:
    crawler = create_autospec(TablesCrawler)
    crawler.snapshot.return_value = [
        Table("hive_metastore", "schema", "delta", "EXTERNAL", "DELTA", location="s3://bucket/delta"),
        Table("hive_metastore", "schema", "serde", "EXTERNAL", "HIVE", location="s3://bucket/serde"),
        Table("hive_metastore", "schema", "xml", "EXTERNAL", "XML", location="s3://bucket/xml"),
    ]
    return crawler


@pytest.fixture
def ctx(ws, installation, tables_crawler) -> AppContext:
    ws.config.host = "https://example.cloud.databricks.com/"
    ws.jobs.run_now.return_value.run_id = 42
    ws.jobs.list_runs.return_value = []
    workspace_info = create_autospec(WorkspaceInfo)
    workspace_info.current.return_value = "my workspace"
    return AppContext(ws).replace(
        installation=installation, tables_crawler=tables_crawler, workspace_info=workspace_info
    )


@pytest.fixture
def client(ctx) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_service] = lambda: UcxAppService(ctx, "obo")
    return TestClient(app)


def test_status(client, ws) -> None:
    response = client.get("/api/status")

    assert response.status_code == 200
    body = response.json()
    assert body["host"] == "https://example.cloud.databricks.com"
    assert body["user"] == "me@example.com"
    assert body["auth_mode"] == "obo"
    assert body["inventory_database"] == "ucx"
    assert {w["step"] for w in body["workflows"]} == {
        "assessment",
        "migrate-tables",
        "migrate-external-hiveserde-tables-in-place-experimental",
        "migrate-external-tables-ctas",
    }


def test_run_assessment_starts_without_waiting(client, ws) -> None:
    response = client.post("/api/assessment/run")

    assert response.status_code == 200
    assert response.json() == {
        "workflow": "assessment",
        "run_id": 42,
        "run_url": "https://example.cloud.databricks.com/#job/1/runs/42",
    }
    ws.jobs.run_now.assert_called_once_with(1, job_parameters=None)
    ws.jobs.wait_get_run_job_terminated_or_skipped.assert_not_called()


def test_run_assessment_force_refresh(client, ws) -> None:
    response = client.post("/api/assessment/run", json={"force_refresh": True})

    assert response.status_code == 200
    ws.jobs.run_now.assert_called_once_with(1, job_parameters={"force_refresh": "true"})


def test_run_workflow_not_installed(client, installation) -> None:
    installation.save({"resources": {"jobs": {}, "dashboards": {}}}, filename="state.json")

    response = client.post("/api/assessment/run")

    assert response.status_code == 404
    assert "not installed" in response.json()["detail"]


def test_assessment_dashboard(client) -> None:
    response = client.get("/api/assessment/dashboard")

    assert response.status_code == 200
    assert response.json() == {
        "dashboard_id": "dash-1",
        "url": "https://example.cloud.databricks.com/dashboardsv3/dash-1/published",
        "embed_url": "https://example.cloud.databricks.com/embed/dashboardsv3/dash-1",
    }


def test_assessment_dashboard_missing(client, installation) -> None:
    installation.save({"resources": {"jobs": {}, "dashboards": {}}}, filename="state.json")

    response = client.get("/api/assessment/dashboard")

    assert response.status_code == 404


def test_get_mapping(client) -> None:
    response = client.get("/api/mapping")

    assert response.status_code == 200
    assert response.json() == {"rules": [{**RULE, "recon_tolerance_percent": 0, "compare_rows": False}]}


def test_get_mapping_missing_returns_bad_request(client, ctx) -> None:
    ctx.replace(installation=MockInstallation({"config.yml": CONFIG}))

    response = client.get("/api/mapping")

    assert response.status_code == 400
    assert "create-table-mapping" in response.json()["detail"]


def test_create_mapping_from_assessment(client, installation) -> None:
    response = client.post("/api/mapping")

    assert response.status_code == 200
    rules = response.json()["rules"]
    assert [rule["src_table"] for rule in rules] == ["delta", "serde", "xml"]
    assert {rule["catalog_name"] for rule in rules} == {"my_workspace"}
    assert len(installation.load(list[Rule], filename="mapping.csv")) == 3


def test_create_mapping_without_assessment(client, tables_crawler) -> None:
    tables_crawler.snapshot.return_value = []

    response = client.post("/api/mapping")

    assert response.status_code == 400
    assert "No tables found" in response.json()["detail"]


def test_save_mapping(client, installation) -> None:
    edited = {**RULE, "catalog_name": "new_catalog", "dst_table": "renamed"}

    response = client.put("/api/mapping", json={"rules": [edited]})

    assert response.status_code == 200
    saved = installation.load(list[Rule], filename="mapping.csv")
    assert saved[0].catalog_name == "new_catalog"
    assert saved[0].dst_table == "renamed"


def test_save_mapping_rejects_duplicate_destination(client, installation) -> None:
    other = {**RULE, "src_table": "other"}

    response = client.put("/api/mapping", json={"rules": [RULE, other]})

    assert response.status_code == 422
    assert "Destination table used more than once: catalog.schema.table" in response.json()["detail"]
    assert installation.load(list[Rule], filename="mapping.csv") == [Rule(**RULE)]


def test_save_mapping_rejects_duplicate_source(client) -> None:
    other = {**RULE, "dst_table": "other"}

    response = client.put("/api/mapping", json={"rules": [RULE, other]})

    assert response.status_code == 422
    assert "Source table mapped more than once" in response.json()["detail"]


def test_save_mapping_rejects_empty_fields(client) -> None:
    response = client.put("/api/mapping", json={"rules": [{**RULE, "catalog_name": ""}]})

    assert response.status_code == 422


def test_migration_precheck(client, verify) -> None:
    response = client.get("/api/migration/precheck")

    assert response.status_code == 200
    assert response.json() == {
        "total_tables": 3,
        "hiveserde_tables": 1,
        "external_ctas_tables": 1,
        "prerequisites_error": None,
    }
    verify.verify.assert_called_once_with()


def test_migration_precheck_reports_unmet_prerequisites(client, verify) -> None:
    verify.verify.side_effect = RuntimeWarning("UCX catalog not configured")

    response = client.get("/api/migration/precheck")

    assert response.status_code == 200
    assert response.json()["prerequisites_error"] == "UCX catalog not configured"


@pytest.fixture
def verify(ctx) -> VerifyProgressTracking:
    verify_progress_tracking = create_autospec(VerifyProgressTracking)
    verify_progress_tracking.verify.return_value = None  # prerequisites met
    ctx.replace(verify_progress_tracking=verify_progress_tracking)
    return verify_progress_tracking


def test_run_migration_defaults_to_migrate_tables(client, ws, verify) -> None:
    ws.jobs.submit.return_value.response.run_id = 77
    ws.jobs.get_run.return_value = Run(run_id=77, run_page_url="https://example.cloud.databricks.com/#job/9/run/77")

    response = client.post("/api/migration/run")

    assert response.status_code == 200
    assert response.json() == {
        "run_id": 77,
        "run_url": "https://example.cloud.databricks.com/#job/9/run/77",
        "workflows": ["migrate-tables"],
    }
    ws.jobs.submit.assert_called_once_with(
        run_name="[UCX] migrate-tables (requested by me@example.com)",
        tasks=[SubmitTask(task_key="migrate_tables", run_job_task=RunJobTask(job_id=2))],
    )
    verify.verify.assert_called_once()
    ws.jobs.run_now.assert_not_called()


def test_run_migration_chains_selected_workflows_in_order(client, ws, verify) -> None:
    ws.jobs.submit.return_value.response.run_id = 77
    ws.jobs.get_run.return_value = Run(run_id=77)

    response = client.post(
        "/api/migration/run", json={"migrate_tables": False, "hiveserde_in_place": True, "external_ctas": True}
    )

    assert response.status_code == 200
    assert response.json()["workflows"] == [
        "migrate-external-hiveserde-tables-in-place-experimental",
        "migrate-external-tables-ctas",
    ]
    assert response.json()["run_url"] == "https://example.cloud.databricks.com/#job/runs/77"
    tasks = ws.jobs.submit.call_args.kwargs["tasks"]
    assert tasks == [
        SubmitTask(
            task_key="migrate_external_hiveserde_tables_in_place_experimental",
            run_job_task=RunJobTask(job_id=3),
        ),
        SubmitTask(
            task_key="migrate_external_tables_ctas",
            run_job_task=RunJobTask(job_id=4),
            depends_on=[TaskDependency(task_key="migrate_external_hiveserde_tables_in_place_experimental")],
        ),
    ]


def test_run_migration_requires_a_selection(client, ws, verify) -> None:
    response = client.post("/api/migration/run", json={"migrate_tables": False})

    assert response.status_code == 422
    verify.verify.assert_not_called()
    ws.jobs.submit.assert_not_called()


def test_run_migration_workflow_not_installed(client, ws, verify, installation) -> None:
    installation.save({"resources": {"jobs": {"migrate-tables": "2"}}}, filename="state.json")

    response = client.post("/api/migration/run", json={"external_ctas": True})

    assert response.status_code == 404
    assert "migrate-external-tables-ctas" in response.json()["detail"]
    ws.jobs.submit.assert_not_called()


def test_run_migration_prerequisites_not_met(client, ws, verify) -> None:
    verify.verify.side_effect = RuntimeWarning("UCX catalog not configured")

    response = client.post("/api/migration/run")

    assert response.status_code == 409
    assert response.json() == {"detail": "UCX catalog not configured"}
    ws.jobs.submit.assert_not_called()


def test_databricks_not_found_maps_to_404(client, tables_crawler) -> None:
    tables_crawler.snapshot.side_effect = NotFound("schema ucx not found")

    response = client.get("/api/migration/precheck")

    assert response.status_code == 404
    assert response.json() == {"detail": "schema ucx not found"}


def test_api_only_when_frontend_missing(tmp_path) -> None:
    app = create_app(static_dir=tmp_path / "missing")

    assert TestClient(app).get("/").status_code == 404


def test_serves_frontend(tmp_path) -> None:
    (tmp_path / "index.html").write_text("<html>ucx</html>")
    app = create_app(static_dir=tmp_path)

    response = TestClient(app).get("/")

    assert response.status_code == 200
    assert "ucx" in response.text


def test_active_runs(client, ws) -> None:
    assessment_run = BaseRun(
        job_id=1,
        run_id=10,
        start_time=1_700_000_000_000,
        state=RunState(life_cycle_state=RunLifeCycleState.RUNNING, state_message="In run"),
    )
    chained_run = BaseRun(
        run_id=20,
        run_name="[UCX] migrate-tables -> migrate-external-tables-ctas",
        run_page_url="https://example.cloud.databricks.com/#job/9/run/20",
        state=RunState(life_cycle_state=RunLifeCycleState.PENDING),
    )
    unrelated_submit_run = BaseRun(run_id=30, run_name="someone else's run")

    def list_runs(*, job_id=None, run_type=None, active_only=None):
        assert active_only
        if run_type == RunType.SUBMIT_RUN:
            return [chained_run, unrelated_submit_run]
        if job_id == 1:
            return [assessment_run]
        if job_id == 3:
            raise InvalidParameterValue("Job 3 does not exist")
        return []

    ws.jobs.list_runs.side_effect = list_runs

    response = client.get("/api/runs/active")

    assert response.status_code == 200
    assert response.json() == {
        "runs": [
            {
                "name": "assessment",
                "run_id": 10,
                "state": "RUNNING",
                "state_message": "In run",
                "started_at": "2023-11-14T22:13:20+00:00",
                "run_url": "https://example.cloud.databricks.com/#job/1/runs/10",
            },
            {
                "name": "migrate-tables -> migrate-external-tables-ctas",
                "run_id": 20,
                "state": "PENDING",
                "state_message": "",
                "started_at": None,
                "run_url": "https://example.cloud.databricks.com/#job/9/run/20",
            },
        ]
    }


def test_active_runs_none(client) -> None:
    response = client.get("/api/runs/active")

    assert response.status_code == 200
    assert response.json() == {"runs": []}


def test_run_status_of_finished_ucx_workflow(client, ws) -> None:
    ws.jobs.get_run.return_value = Run(
        job_id=1,
        run_id=10,
        state=RunState(life_cycle_state=RunLifeCycleState.TERMINATED, result_state=RunResultState.SUCCESS),
    )

    response = client.get("/api/runs/10")

    assert response.status_code == 200
    assert response.json()["name"] == "assessment"
    assert response.json()["state"] == "SUCCESS"
    ws.jobs.get_run.assert_called_once_with(10)


def test_run_status_of_chained_run(client, ws) -> None:
    ws.jobs.get_run.return_value = Run(
        run_id=20,
        run_name="[UCX] migrate-tables",
        state=RunState(life_cycle_state=RunLifeCycleState.INTERNAL_ERROR, result_state=RunResultState.FAILED),
    )

    response = client.get("/api/runs/20")

    assert response.status_code == 200
    assert response.json()["name"] == "migrate-tables"
    assert response.json()["state"] == "FAILED"


def test_run_status_rejects_non_ucx_runs(client, ws) -> None:
    ws.jobs.get_run.return_value = Run(job_id=999, run_id=30, run_name="payroll")

    response = client.get("/api/runs/30")

    assert response.status_code == 404
    assert response.json() == {"detail": "Run 30 is not a UCX run"}


def test_jobs_api_uses_the_jobs_client(ws, installation, tables_crawler) -> None:
    jobs_ws = create_autospec(WorkspaceClient)
    jobs_ws.config.host = "https://example.cloud.databricks.com"
    jobs_ws.jobs.run_now.return_value.run_id = 42
    jobs_ws.jobs.list_runs.return_value = []
    ws.config.host = "https://example.cloud.databricks.com"
    hybrid_ctx = AppContext(ws, jobs_ws=jobs_ws).replace(installation=installation, tables_crawler=tables_crawler)
    app = create_app()
    app.dependency_overrides[get_service] = lambda: UcxAppService(hybrid_ctx, "obo")
    hybrid_client = TestClient(app)

    assert hybrid_client.post("/api/assessment/run").status_code == 200
    assert hybrid_client.get("/api/runs/active").status_code == 200
    assert hybrid_client.get("/api/status").json()["user"] == "me@example.com"

    jobs_ws.jobs.run_now.assert_called_once()
    jobs_ws.jobs.list_runs.assert_called()
    ws.jobs.run_now.assert_not_called()
    ws.jobs.list_runs.assert_not_called()
    jobs_ws.current_user.me.assert_not_called()  # the user identity comes from the user's token

from unittest import mock
from unittest.mock import create_autospec

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import LegacyQuery
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.framework.owners import AdministratorLocator, LegacyQueryOwnership
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.queries import QueryLinter, QueryProblem, QueryProblemOwnership
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


def test_query_problem_from_dict() -> None:
    query_problem = QueryProblem.from_dict(
        {
            "dashboard_id": "dashboard_id",
            "dashboard_parent": "dashboard_parent",
            "dashboard_name": "dashboard_name",
            "query_id": "query_id",
            "query_parent": "query_parent",
            "query_name": "query_name",
            "code": "code",
            "message": "message",
        }
    )

    assert query_problem.dashboard_id == "dashboard_id"
    assert query_problem.dashboard_parent == "dashboard_parent"
    assert query_problem.dashboard_name == "dashboard_name"
    assert query_problem.query_id == "query_id"
    assert query_problem.query_parent == "query_parent"
    assert query_problem.query_name == "query_name"
    assert query_problem.code == "code"
    assert query_problem.message == "message"


def test_query_problem_ownership_defers_to_legacy_query_ownership() -> None:
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "John Doe"
    legacy_query_ownership = create_autospec(LegacyQueryOwnership)
    legacy_query_ownership.owner_of.side_effect = lambda id_: "Mary Jane" if id_ == "query_id" else None
    ownership = QueryProblemOwnership(administrator_locator, legacy_query_ownership)
    query_problem = QueryProblem(
        "dashboard_id",
        "dashboard_parent",
        "dashboard_name",
        "query_id",
        "query_parent",
        "query_name",
        "code",
        "message",
    )

    owner = ownership.owner_of(query_problem)

    assert owner == "Mary Jane"
    administrator_locator.get_workspace_administrator.assert_not_called()
    legacy_query_ownership.owner_of.assert_called_once_with("query_id")


@pytest.mark.parametrize(
    "name, query, dfsa_paths, is_read, is_write",
    [
        ("simple", "SELECT * from dual", [], False, False),
        (
            "location",
            "CREATE TABLE hive_metastore.indices_historical_data.sp_500 LOCATION 's3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/'",
            ["s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/"],
            False,
            True,
        ),
    ],
)
def test_query_linter_collects_dfsas_from_queries(
    name, query, dfsa_paths, is_read, is_write, migration_index, mock_backend
) -> None:
    ws = create_autospec(WorkspaceClient)
    dfsa_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    query = LegacyQuery.from_dict({"parent": "workspace", "name": name, "query": query})
    linter = QueryLinter(ws, mock_backend, "test", migration_index, dfsa_crawler, used_tables_crawler, None)
    dfsas = linter.collect_dfsas_from_query("no-dashboard-id", query)
    ws.assert_not_called()
    dfsa_crawler.assert_not_called()
    used_tables_crawler.assert_not_called()
    assert set(dfsa.path for dfsa in dfsas) == set(dfsa_paths)
    assert all(dfsa.is_read == is_read for dfsa in dfsas)
    assert all(dfsa.is_write == is_write for dfsa in dfsas)


def test_query_linter_refresh_report_writes_query_problems(migration_index, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dfsa_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    linter = QueryLinter(ws, mock_backend, "test", migration_index, dfsa_crawler, used_tables_crawler, None)

    linter.refresh_report()

    assert mock_backend.has_rows_written_for("`hive_metastore`.`test`.`query_problems`")
    ws.dashboards.list.assert_called_once()
    dfsa_crawler.assert_not_called()
    used_tables_crawler.assert_not_called()


def test_lints_queries(migration_index, mock_backend) -> None:
    with mock.patch("databricks.labs.ucx.source_code.queries.Redash") as mocked_redash:
        query = LegacyQuery(id="123", query="SELECT * from nowhere")
        mocked_redash.get_queries_from_dashboard.return_value = [query]
        ws = create_autospec(WorkspaceClient)
        dfsa_crawler = create_autospec(DirectFsAccessCrawler)
        used_tables_crawler = create_autospec(UsedTablesCrawler)
        linter = QueryLinter(ws, mock_backend, "test", migration_index, dfsa_crawler, used_tables_crawler, ["1"])
        linter.refresh_report()

        assert mock_backend.has_rows_written_for("`hive_metastore`.`test`.`query_problems`")
        ws.dashboards.list.assert_not_called()
        dfsa_crawler.assert_not_called()
        used_tables_crawler.assert_not_called()


def test_query_linter_snapshot(migration_index, mock_backend) -> None:
    column_names = (
        "dashboard_id",
        "dashboard_parent",
        "dashboard_name",
        "query_id",
        "query_parent",
        "query_name",
        "code",
        "message",
    )
    rows = MockBackend.rows(*column_names)
    mock_backend = MockBackend(rows={"SELECT \\* FROM `hive_metastore`.`test`.`query_problems`": rows[[column_names]]})
    ws = create_autospec(WorkspaceClient)
    dfsa_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    linter = QueryLinter(ws, mock_backend, "test", migration_index, dfsa_crawler, used_tables_crawler, None)

    snapshot = list(linter.snapshot())

    assert len(snapshot) == 1
    query_problem = snapshot[0]
    assert query_problem.dashboard_id == "dashboard_id"
    assert query_problem.dashboard_parent == "dashboard_parent"
    assert query_problem.dashboard_name == "dashboard_name"
    assert query_problem.query_id == "query_id"
    assert query_problem.query_parent == "query_parent"
    assert query_problem.query_name == "query_name"
    assert query_problem.code == "code"
    assert query_problem.message == "message"

    ws.assert_not_called()
    dfsa_crawler.dump_all.assert_not_called()
    used_tables_crawler.dump_all.assert_not_called()

from unittest import mock
from unittest.mock import create_autospec

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import LegacyQuery

from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.queries import QueryLinter
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


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
def test_query_linter_collects_dfsas_from_queries(name, query, dfsa_paths, is_read, is_write, migration_index) -> None:
    ws = create_autospec(WorkspaceClient)
    dfsa_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    query = LegacyQuery.from_dict({"parent": "workspace", "name": name, "query": query})
    linter = QueryLinter(ws, migration_index, dfsa_crawler, used_tables_crawler, None)
    dfsas = linter.collect_dfsas_from_query("no-dashboard-id", query)
    ws.assert_not_called()
    dfsa_crawler.assert_not_called()
    used_tables_crawler.assert_not_called()
    assert set(dfsa.path for dfsa in dfsas) == set(dfsa_paths)
    assert all(dfsa.is_read == is_read for dfsa in dfsas)
    assert all(dfsa.is_write == is_write for dfsa in dfsas)


def test_query_liner_refresh_report_writes_query_problems(migration_index, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dfsa_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    linter = QueryLinter(ws, migration_index, dfsa_crawler, used_tables_crawler, None)

    linter.refresh_report(mock_backend, inventory_database="test")

    assert mock_backend.has_rows_written_for("`test`.query_problems")
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
        linter = QueryLinter(ws, migration_index, dfsa_crawler, used_tables_crawler, ["1"])
        linter.refresh_report(mock_backend, inventory_database="test")

        assert mock_backend.has_rows_written_for("`test`.query_problems")
        ws.dashboards.list.assert_not_called()
        dfsa_crawler.assert_not_called()
        used_tables_crawler.assert_not_called()

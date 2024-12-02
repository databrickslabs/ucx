from unittest.mock import create_autospec

import pytest

from databricks.labs.lsql.backends import Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import LegacyQuery

from databricks.labs.ucx.assessment.dashboards import RedashDashboard, RedashDashboardCrawler
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
def test_query_linter_collects_dfsas_from_queries(
    name, query, dfsa_paths, is_read, is_write, migration_index, mock_backend
) -> None:
    ws = create_autospec(WorkspaceClient)
    dfsa_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    dashboard_crawler = create_autospec(RedashDashboardCrawler)
    query = LegacyQuery.from_dict({"parent": "workspace", "name": name, "query": query})
    linter = QueryLinter(
        ws,
        mock_backend,
        "test",
        migration_index,
        dfsa_crawler,
        used_tables_crawler,
        [dashboard_crawler],
    )

    dfsas = linter.collect_dfsas_from_query("no-dashboard-id", query)

    assert set(dfsa.path for dfsa in dfsas) == set(dfsa_paths)
    assert all(dfsa.is_read == is_read for dfsa in dfsas)
    assert all(dfsa.is_write == is_write for dfsa in dfsas)
    ws.assert_not_called()
    dfsa_crawler.assert_not_called()
    used_tables_crawler.assert_not_called()
    dashboard_crawler.snapshot.assert_not_called()


def test_query_linter_refresh_report_writes_query_problems(migration_index, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dfsa_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    dashboard_crawler = create_autospec(RedashDashboardCrawler)
    linter = QueryLinter(
        ws,
        mock_backend,
        "test",
        migration_index,
        dfsa_crawler,
        used_tables_crawler,
        [dashboard_crawler],
    )

    linter.refresh_report()

    assert mock_backend.has_rows_written_for("`hive_metastore`.`test`.`query_problems`")
    ws.queries_legacy.list.assert_called_once()
    dfsa_crawler.assert_not_called()
    used_tables_crawler.assert_not_called()
    dashboard_crawler.snapshot.assert_called_once()


def test_lints_queries(migration_index, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.queries_legacy.get.return_value = LegacyQuery(
        id="qid",
        name="qname",
        parent="qparent",
        query="SELECT * FROM old.things",
    )
    dfsa_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    dashboard_crawler = create_autospec(RedashDashboardCrawler)
    dashboard_crawler.snapshot.return_value = [RedashDashboard("did", "dname", "dparent", query_ids=["qid"])]
    linter = QueryLinter(
        ws,
        mock_backend,
        "test",
        migration_index,
        dfsa_crawler,
        used_tables_crawler,
        [dashboard_crawler],
    )

    linter.refresh_report()

    rows = mock_backend.rows_written_for("`hive_metastore`.`test`.`query_problems`", "overwrite")
    assert rows == [
        Row(
            dashboard_id="did",
            dashboard_parent="dparent",
            dashboard_name="dname",
            query_id="qid",
            query_parent="qparent",
            query_name="qname",
            code="table-migrated-to-uc",
            message="Table old.things is migrated to brand.new.stuff in Unity Catalog",
        )
    ]
    dfsa_crawler.assert_not_called()
    used_tables_crawler.assert_not_called()
    dashboard_crawler.snapshot.assert_called_once()

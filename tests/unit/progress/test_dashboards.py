from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend, Row

from databricks.labs.ucx.__about__ import __version__ as ucx_version
from databricks.labs.ucx.assessment.dashboards import Dashboard
from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.dashboards import DashboardProgressEncoder
from databricks.labs.ucx.source_code.base import LineageAtom
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccess, DirectFsAccessCrawler
from databricks.labs.ucx.source_code.used_table import UsedTable, UsedTablesCrawler


@pytest.mark.parametrize(
    "expected",
    [
        Row(
            workspace_id=123456789,
            job_run_id=1,
            object_type="Dashboard",
            object_id=["did1"],
            data={"id": "did1", "query_ids": "[]", "tags": "[]"},
            failures=[],
            owner="cor",
            ucx_version=ucx_version,
        ),
        Row(
            workspace_id=123456789,
            job_run_id=1,
            object_type="Dashboard",
            object_id=["did2"],
            data={"id": "did2", "query_ids": "[]", "tags": "[]"},
            failures=["[sql-parse-error] my_query (did2/qid1) : Could not parse SQL"],
            owner="cor",
            ucx_version=ucx_version,
        ),
        Row(
            workspace_id=123456789,
            job_run_id=1,
            object_type="Dashboard",
            object_id=["did3"],
            data={"id": "did3", "query_ids": "[]", "tags": "[]"},
            failures=[
                "[direct-filesystem-access] query (did3/qid1) : "
                "The use of direct filesystem references is deprecated: dfsa:/path/to/data/",
            ],
            owner="cor",
            ucx_version=ucx_version,
        ),
        Row(
            workspace_id=123456789,
            job_run_id=1,
            object_type="Dashboard",
            object_id=["did4"],
            data={"id": "did4", "query_ids": "[]", "tags": "[]"},
            failures=["Used by TABLE: hive_metastore.schema.table"],
            owner="cor",
            ucx_version=ucx_version,
        ),
    ],
)
def test_dashboard_progress_encoder(expected: Row) -> None:
    # The Mockbackend.fetch ignores the catalog and schema keyword arguments
    mock_backend = MockBackend(
        rows={
            # Expect a query problem for dashboard two
            "SELECT \\* FROM query_problems": [
                Row(
                    dashboard_id="did2",
                    dashboard_parent="dashbards/parent",
                    dashboard_name="my_dashboard",
                    query_id="qid1",
                    query_parent="queries/parent",
                    query_name="my_query",
                    code="sql-parse-error",
                    message="Could not parse SQL",
                )
            ],
            # A Hive table used by dashboard 4
            "SELECT \\* FROM objects_snapshot WHERE object_type = 'Table'": [
                Row(
                    workspace_id=123456789,
                    job_run_id=1,
                    object_type="Table",
                    object_id=["hive_metastore", "schema", "table"],
                    data={
                        "catalog": "hive_metastore",
                        "database": "schema",
                        "name": "table",
                        "object_type": "TABLE",
                        "table_format": "DELTA",
                    },
                    failures=["Used by TABLE: hive_metastore.schema.table"],
                    owner="cor",
                    ucx_version=ucx_version,
                )
            ],
        }
    )
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "cor"
    # Expect a direct filesystem failure message for dashboard 3
    dfsa = DirectFsAccess(
        source_id="/path/to/write_dfsa.py",
        source_lineage=[
            LineageAtom(
                object_type="DASHBOARD",
                object_id="did3",
                other={"parent": "parent", "name": "dashboard"},
            ),
            LineageAtom(object_type="QUERY", object_id="did3/qid1", other={"name": "query"}),
        ],
        path="dfsa:/path/to/data/",
        is_read=False,
        is_write=True,
    )
    direct_fs_access_crawler = create_autospec(DirectFsAccessCrawler)
    direct_fs_access_crawler.snapshot.return_value = [dfsa]
    # Expect a used Hive table failure message for dashboard 4
    used_table = UsedTable(
        catalog_name="hive_metastore",
        schema_name="schema",
        table_name="table",
        source_id="did4/qid1",
        source_lineage=[
            LineageAtom(
                object_type="DASHBOARD",
                object_id="did4",
                other={"parent": "parent", "name": "dashboard"},
            ),
            LineageAtom(object_type="QUERY", object_id="did4/qid1", other={"name": "query"}),
        ],
    )
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.snapshot.return_value = [used_table]
    encoder = DashboardProgressEncoder(
        mock_backend,
        ownership,
        [direct_fs_access_crawler],
        [used_tables_crawler],
        inventory_database="inventory",
        job_run_id=1,
        workspace_id=123456789,
        catalog="test",
    )
    dashboard = Dashboard(expected.object_id[0])

    encoder.append_inventory_snapshot([dashboard])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert rows == [expected]
    ownership.owner_of.assert_called_once()
    direct_fs_access_crawler.snapshot.assert_called_once()
    used_tables_crawler.snapshot.assert_called_once()

import logging

from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.account.aggregate import AccountAggregate


def test_account_aggregate_finds_no_overlapping_tables(caplog, acc):
    account_aggregate = AccountAggregate(AccountWorkspaces(acc))
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.aggregate"):
        conflicts = account_aggregate.validate_table_locations()
    assert "Overlapping table locations" not in caplog.text
    assert not conflicts


def test_account_aggregate_finds_overlapping_tables(caplog, acc, ws, sql_backend, inventory_schema):
    tables = [
        Table("hive_metastore", "d1", "t1", "EXTERNAL", "DELTA", "s3://test_location/table1/"),
        Table("hive_metastore", "d1", "t2", "EXTERNAL", "DELTA", "s3://test_location/table1/"),
    ]
    sql_backend.save_table(f"{inventory_schema}.tables", tables, Table)

    account_workspaces = AccountWorkspaces(acc)
    w_ctx = WorkspaceContext(ws).replace(sql_backend=sql_backend, config=WorkspaceConfig(inventory_schema))
    account_aggregate = AccountAggregate(
        account_workspaces,
        lambda w: w_ctx if w.get_workspace_id() == ws.get_workspace_id() else WorkspaceContext(w),
    )
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.aggregate"):
        conflicts = account_aggregate.validate_table_locations()
    assert "Overlapping table locations" in caplog.text
    assert len(conflicts) == 0
    assert "hive_metastore.d1.t1" in str(conflicts[0][0])
    assert "hive_metastore.d1.t2" in str(conflicts[0][1])

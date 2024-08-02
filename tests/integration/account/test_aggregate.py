import logging

from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.account.aggregate import AccountAggregate


def test_account_aggregate_logs_overlapping_tables(caplog, sql_backend, acc, installation_ctx):
    w_ctx = WorkspaceContext(installation_ctx.workspace_client).replace(sql_backend=sql_backend)
    tables = [
        Table("hive_metastore", "d1", "t1", "EXTERNAL", "DELTA", "s3://test_location/test1/table1"),
        Table("hive_metastore", "d1", "t2", "EXTERNAL", "DELTA", "s3://test_location/test2/table2"),
    ]
    w_ctx.sql_backend.save_table(f"{installation_ctx.config.inventory_database}.tables", tables, Table)
    i_ctx = installation_ctx.replace(sql_backend=sql_backend)
    i_ctx.workspace_installation.run()

    account_workspaces = AccountWorkspaces(acc)
    account_aggregate = AccountAggregate(account_workspaces, lambda _: i_ctx)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.aggregate"):
        account_aggregate.validate()
    assert "Overlapping table locations" in caplog.text

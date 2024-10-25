import dataclasses

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.table_migration_status import (
    TableMigrationStatus,
    TableMigrationStatusRefresher,
)
from databricks.labs.ucx.hive_metastore.ownership import TableMigrationOwnership


def test_table_migration_ownership(ws, runtime_ctx, inventory_schema, sql_backend) -> None:
    """Verify the ownership can be determined for crawled table-migration records."""

    # A table for which a migration record will be produced.
    table = runtime_ctx.make_table()

    # Use the crawlers to produce the migration record.
    tables_crawler = TablesCrawler(sql_backend, schema=inventory_schema, include_databases=[table.schema_name])
    table_records = tables_crawler.snapshot(force_refresh=True)
    migration_status_refresher = TableMigrationStatusRefresher(ws, sql_backend, table.schema_name, tables_crawler)
    migration_records = migration_status_refresher.snapshot(force_refresh=True)

    # Find the crawled records for the table we made.
    table_record = next(record for record in table_records if record.full_name == table.full_name)

    def is_migration_record_for_table(record: TableMigrationStatus) -> bool:
        return record.src_schema == table.schema_name and record.src_table == table.name

    table_migration_record = next(record for record in migration_records if is_migration_record_for_table(record))
    # Make a synthetic record that doesn't correspond to anything in the inventory.
    synthetic_record = dataclasses.replace(table_migration_record, src_table="does_not_exist")

    # Verify for the table that the table owner and the migration status are a match.
    table_ownership = runtime_ctx.table_ownership
    table_migration_ownership = TableMigrationOwnership(tables_crawler, table_ownership)
    assert table_migration_ownership.owner_of(table_migration_record) == table_ownership.owner_of(table_record)

    # Verify the owner of the migration record that corresponds to an unknown table.
    workspace_administrator = runtime_ctx.administrator_locator.get_workspace_administrator()
    assert table_migration_ownership.owner_of(synthetic_record) == workspace_administrator

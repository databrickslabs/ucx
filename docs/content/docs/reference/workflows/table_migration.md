## Table migration workflows

This section lists the workflows that migrate tables and views. See [this section](#migrate-hive-metastore-data-objects)
for deciding which workflow to run and additional context for migrating tables.

### Migrate tables

The general table migration workflow `migrate-tables` migrates all tables and views using default strategies.

1. `migrate_external_tables_sync` : This step migrates the external tables that are supported by `SYNC` command.
2. `migrate_dbfs_root_delta_tables` : This step migrates delta tables stored in DBFS root using the `DEEP CLONE`
   command.
3. `migrate_dbfs_root_non_delta_tables` : This step migrates non-delta tables stored in DBFS root using the `CREATE
   TABLE AS SELECT * FROM` command.
4. `migrate_views` : This step migrates views using the `CREATE VIEW` command.
5. `update_migration_status` : Refresh the migration status of all data objects.

### Migrate external Hive SerDe tables

The experimental table migration workflow `migrate-external-hiveserde-tables-in-place-experimental` migrates tables that
support the `SYNC AS EXTERNAL` command.

1. `migrate_hive_serde_in_place` : This step migrates the Hive SerDe tables that are supported by `SYNC AS EXTERNAL`
   command.
2. `migrate_views` : This step migrates views using the `CREATE VIEW` command.
3. `update_migration_status` : Refresh the migration status of all data objects.

### Migrate external tables CTAS

The table migration workflow `migrate-external-tables-ctas` migrates tables with the `CREATE TABLE AS SELECT * FROM`
command.

1. `migrate_other_external_ctas` This step migrates the Hive Serde tables using the `CREATE TABLE AS SELECT * FROM`
   command.
2. `migrate_hive_serde_ctas` : This step migrates the Hive Serde tables using the `CREATE TABLE AS SELECT * FROM`
   command.
4. `migrate_views` : This step migrates views using the `CREATE VIEW` command.
4. `update_migration_status` : Refresh the migration status of all data objects.

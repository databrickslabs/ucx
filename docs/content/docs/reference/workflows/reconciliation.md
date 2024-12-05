## Post-migration data reconciliation workflow

The `migrate-data-reconciliation` workflow validates the integrity of the migrated tables and persists its results in
the `recon_results` table. The workflow compares the following between the migrated Hive metastore and its UC
counterpart table:
- `Schema` : See this result in the `schema_matches` column.
- `Column by column` : See this result in the `column_comparison` column.
- `Row counts` : If the row count is within the reconciliation threshold (defaults to 5%), the `data_matches` column is
  set to `true`, otherwise it is set to `false`.
- `Rows` : If the `compare_rows` flag is set to `true`, rows are compared using a hash comparison. Number of missing
  rows are stored in the `source_missing_count` and `target_missing_count` column, respectively.

The output is processed and displayed in the migration dashboard using the in `reconciliation_results` view.

![reconciliation results](docs/recon_results.png)

### [LEGACY] Scan tables in mounts Workflow
#### <b>Always run this workflow AFTER the assessment has finished</b>
- This experimental workflow attempts to find all Tables inside mount points that are present on your workspace.
- If you do not run this workflow, then `migrate-tables-in-mounts-experimental` won't do anything.
- It writes all results to `hive_metastore.<inventory_database>.tables`, you can query those tables found by filtering on database values that starts with `mounted_`
- This command is incremental, meaning that each time you run it, it will overwrite the previous tables in mounts found.
- Current format are supported:
  - DELTA - PARQUET - CSV - JSON
  - Also detects partitioned DELTA and PARQUET
- You can configure these workflows with the following options available on conf.yml:
  - include_mounts : A list of mount points to scans, by default the workflow scans for all mount points
  - exclude_paths_in_mount : A list of paths to exclude in all mount points
  - include_paths_in_mount : A list of paths to include in all mount points

### [LEGACY] Migrate tables in mounts Workflow
- An experimental workflow that migrates tables in mount points using a `CREATE TABLE` command, optinally sets a default tables owner if provided in `default_table_owner` conf parameter.
- You must do the following in order to make this work:
  - run the Assessment [workflow](#assessment-workflow)
  - run the scan tables in mounts [workflow](#EXPERIMENTAL-scan-tables-in-mounts-workflow)
  - run the [`create-table-mapping` command](#create-table-mapping-command)
    - or manually create a `mapping.csv` file in Workspace -> Applications -> ucx

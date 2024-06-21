---
height: 6
---

## Table migration status

The two widgets on the right show high-level summary of the table migration. The first widget shows the migration
progress, and the second widget shows the data reconciliation results.


The table below assists with verifying if, how the tables are migrated and their correctness. It can be filtered on the
table name and migration status. Next to table metadata, the table shows:
- The table name before migrating
- The migration status
- If migrated
  - The table name after migrating
  - Whether schema matches and whether data matches
  - Number of rows in the original table and the target table
  - Number of rows that are in the original table but not in the target table, and vice versa (only when compare_rows is `True`)

The table migration status is updated after running [table migration workflow](https://github.com/databrickslabs/ucx/blob/main/README.md#table-migration-workflow).

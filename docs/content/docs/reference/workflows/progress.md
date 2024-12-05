## [EXPERIMENTAL] Migration Progress Workflow

The `migration-progress-experimental` workflow updates a subset of the inventory tables to track migration status of
workspace resources that need to be migrated. Besides updating the inventory tables, this workflow tracks the migration
progress by updating the following [UCX catalog](#create-ucx-catalog-command) tables:

- `workflow_runs`: Tracks the status of the workflow runs.

_Note: A subset of the inventory is updated, *not* the complete inventory that is initially gathered by
the [assessment workflow](#assessment-workflow)._

[[back to top](#databricks-labs-ucx)]

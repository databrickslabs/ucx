## [EXPERIMENTAL] Migration Progress Workflow

The `migration-progress-experimental` workflow updates a subset of the inventory tables to track migration status of
workspace resources that need to be migrated. Besides updating the inventory tables, this workflow tracks the migration
progress by updating the following [UCX catalog](docs/reference/commands.md#create-ucx-catalog) tables:

- `workflow_runs`: Tracks the status of the workflow runs.

{{< callout type="info">}}
A subset of the inventory is updated, *not* the complete inventory that is initially gathered by
the [assessment workflow](docs/reference/workflows/assessment.md).
{{< /callout >}}



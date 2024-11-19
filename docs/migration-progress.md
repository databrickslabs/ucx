# Migration progress

UCX tracks migration progress of _business_ resources: workspace resources that contribute to business value.
(The term "business resource" comes from the UCX team and is **not** Databricks terminology.) We identified the
following business resources:

| Workspace resource         | Motivation                                                                                                                  |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| Dashboard                  | Dashboards visualize data models supporting business processes                                                              |
| Job                        | Jobs create data models supporting business process - not exclusively data models used by dashboards.                       |
| Delta Live Table pipelines | Delta Live Table pipelines create data models supporting business process - not exclusively data models used by dashboards. |

## Tracking

UCX [workflows](./README#workflows) track the migration process and populate [migration progress tables](#persistence).

## Persistence

The progress is persisted in the [UCX UC catalog](./README#create-ucx-catalog-command) so that migration progress can be
tracked cross-workspace. The catalog contains the tables below.

### Historical

The [`historical` table](../src/databricks/labs/ucx/progress/install.py) contains historical records of inventory
objects relevant to the migration progress

| Column       | Data type               | Description                                                                        |
|--------------|-------------------------|------------------------------------------------------------------------------------|
| workspace_id | integer                 | The identifier of the workspace where this historical record was generated.        |
| job_run_id   | integer                 | The identifier of the job run that generated this historical record.               |
| object_type  | string                  | The inventory table for which this historical record was generated.                |
| object_id    | list[string]            | The type-specific identifier for the corresponding inventory record.               |
| data         | mapping[string, string] | Type-specific JSON-encoded data of the inventory record.                           |
| failures     | list[string]            | The list of problems associated with the object that this inventory record covers. |
| owner        | string                  | The identity that has ownership of the object.                                     |
| ucx_version  | string                  | The UCX semantic version.                                                          |

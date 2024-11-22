# Migration progress

UCX tracks migration progress of _business_ resources: workspace objects that contribute to business value.
(The term "business resource" comes from the UCX team and is **not** Databricks terminology.) We identified the
following business resources:

| Business resource          | Motivation                                                                                                                 |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------|
| Dashboard                  | Dashboards visualize data models supporting business processes                                                             |
| Job                        | Jobs create data models supporting business process - not exclusively data models used by dashboards                       |
| Delta Live Table pipelines | Delta Live Table pipelines create data models supporting business process - not exclusively data models used by dashboards |

Furthermore, UCX tracks migration of the following Hive and workspace objects:

| Hive or workspace object           |
|------------------------------------|
| Tables and view (Hive data object) |
| Grant                              |
| User defined function (UDF)        |
| Cluster                            |
| Cluster policies                   |

See the [resource index](#resource-index) for more details on the above objects.

## Usage

Use the migration progress through the [migration progress dashboard](../README.md#dashboards) after running the
[(experimental) migration progress workflow](../README.md#experimental-migration-progress-workflow).

### Failures

A [key historical attribute](#historical) in migration progress are the `failures` that show the incompatibility issues
with Unity Catalog. By resolving the failures for an object, UCX flags that object to be Unity Compatible. Thus,
for Hive data objects, this means that the objects are migrated to Unity Catalog.

### Owner

Another [key historical attribute](#historical) in migration progress is the `owner` that shows who owns the object,
thus who is key for making the object Unity Catalog compatible. The ownership is a best effort basis; a concept made
more central in [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/ownership.html).

## Tracking

The [(experimental) migration progress workflow](.../README.md#experimental-migration-progress-workflow) tracks the
migration progress and populates [migration progress tables](#persistence).

### Roll-up to business resources

The migration process' main intent is to track if business resources are migrated to Unity Catalog. UCX rolls up the
failures of dependent resources to the business resources so that the business resources show
the [`failures`](#failures) of the dependent resources.

| Business resource          | Dependent resources                                               |
|----------------------------|-------------------------------------------------------------------|
| Dashboard                  | Queries                                                           |
| Job                        | Cluster, cluster policies, cluster configurations, code resources |
| Delta Live Table pipelines | TBD                                                               |

Similarly, a roll-up for the failures of the Hive and workspace object are done:

| Hive or workspace object           | Dependent resources          |
|------------------------------------|------------------------------|
| Tables and view (Hive data object) | Grants, TableMigrationStatus |
| Grant                              |                              |
| User defined function (UDF)        |                              |
| Cluster                            | Cluster policies             |
| Cluster policies                   |                              |

### Dangling Hive or workspace objects

Hive or workspace objects that are not referenced by [business resources](#roll-up-to-business-resources) are considered
to be _dangling_ objects. For now, these objects are tracked separately, thus not rolled up to business resources.

## Persistence

The progress is persisted in the [UCX UC catalog](../README.md#create-ucx-catalog-command) so that migration progress can be
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

Example historical record:

| workspace_id | job_run_id | object_type | object_id                             | data                                                                                                                                              | failures                           | owner                         | ucx_version |
|--------------|------------|-------------|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------|-------------------------------|-------------|
| 123456789    | 1          | 'Table'     | ['hive_metastore', 'schema', 'table'] | {'database': 'schem', 'name': 'table', 'catalog': 'hive_metastore', 'object_type': 'MANAGED', 'table_format': 'DELTA', 'is_partitioned': 'false'} | ['Used by NOTEBOOK: test/test.py'] | 'cor.zuurmond@databricks.com' | '0.50.0'    |

### Workflow run

The auxiliary [`workflow_runs` table](../src/databricks/labs/ucx/progress/install.py) tracks UCX workflow runs.

| Column               | Data type   | Description                                |
|----------------------|-------------|--------------------------------------------|
| started_at           | dt.datetime | The timestamp of the workflow run start    |
| finished_at          | dt.datetime | The timestamp of the workflow run end      |
| workspace_id         | int         | The workspace id in which the workflow ran |
| workflow_name        | str         | The workflow name that ran                 |
| workflow_id          | int         | The workflow id of the workflow that ran   |
| workflow_run_id      | int         | The workflow run id                        |
| workflow_run_attempt | int         | The workflow run attempt                   |

Example workflow run record:

| started_at                                                                | finished_at                                                               | workspace_id | workflow_name                       | workflow_id | workflow_run_id | workflow_run_attempt |
|---------------------------------------------------------------------------|---------------------------------------------------------------------------|--------------|-------------------------------------|-------------|-----------------|----------------------|
| datetime.datetime(2024, 11, 22, 13, 50, 37, tzinfo=datetime.timezone.utc) | datetime.datetime(2024, 11, 22, 13, 50, 58, tzinfo=datetime.timezone.utc) | 123456789    | 'Migration progress (experimental)' | 123         | 456             | 0                    |

## Resource index

| Hive or workspace object           | Description                                                                                                                | Dependent resources                     |
|------------------------------------|----------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| Redash dashboard                   | The Redash dashboard                                                                                                       | Queries                                 |
| Lakeview dashboard                 | The Lakeview dashboard                                                                                                     | Queries                                 |
| Dashboard                          | The Redash or lakeview dashboard                                                                                           | Queries                                 |
| Job                                | Jobs create data models supporting business process - not exclusively data models used by dashboards                       | Tasks, Cluster                          |
| Job task                           | Job tasks, defined as part of the job definition                                                                           | Code                                    |
| Delta Live Table pipelines         | Delta Live Table pipelines create data models supporting business process - not exclusively data models used by dashboards |                                         |
| Tables and view (Hive data object) | Hive data objects                                                                                                          | Grant, Table migration stats            |
| Grant                              | Data object privileges                                                                                                     | Legacy grant, Interactive cluster grant |
| Legacy grant                       | Legacy Hive grant privileges managed through `GRANT`, `REVOKE` and `DENY` SQL statements                                   |                                         |
| Interactive cluster grant          | Data object privileges inferred through interactive cluster data access                                                    |                                         |
| User defined function (UDF)        | Hive user defined functions                                                                                                | UDF code definition                     |
| Cluster                            | The job cluster, either job or interactive cluster                                                                         |                                         |
| Cluster policies                   | The cluster policies                                                                                                       |                                         |
| Table migration status             | Status of a table or view being migrated to Unity Catalog, or not                                                          |                                         |
| Code                               | Code definitions either Python or SQL                                                                                      |                                         |

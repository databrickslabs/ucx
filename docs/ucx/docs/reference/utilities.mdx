# Utility commands

## `logs` command

```text
$ databricks labs ucx logs [--workflow WORKFLOW_NAME] [--debug]
```

This command displays the logs of the last run of the specified workflow. If no workflow is specified, it displays
the logs of the workflow that was run the last. This command is useful for developers and administrators who want to
check the logs of the last run of a workflow and ensure that it was executed as expected. It can also be used for
debugging purposes when a workflow is not behaving as expected. By default, only `INFO`, `WARNING`, and `ERROR` logs
are displayed. To display `DEBUG` logs, use the `--debug` flag.

[[back to top](#databricks-labs-ucx)]

## `ensure-assessment-run` command

```commandline
databricks labs ucx ensure-assessment-run
```

This command ensures that the [assessment workflow](#assessment-workflow) was run on a workspace.
This command will block until job finishes.
Failed workflows can be fixed with the [`repair-run` command](#repair-run-command). Workflows and their status can be
listed with the [`workflows` command](#workflows-command).

[[back to top](#databricks-labs-ucx)]

## `update-migration-progress` command

```commandline
databricks labs ucx update-migration-progress
```

This command runs the [(experimental) migration progress workflow](#experimental-migration-progress-workflow) to update
the migration status of workspace resources that need to be migrated. It does this by triggering
the `migration-progress-experimental` workflow to run on a workspace and waiting for
it to complete.

Workflows and their status can be listed with the [`workflows` command](#workflows-commandr), while failed workflows can
be fixed with the [`repair-run` command](#repair-run-command).

[[back to top](#databricks-labs-ucx)]

## `repair-run` command

```commandline
databricks labs ucx repair-run --step WORKFLOW_NAME
```

This command repairs a failed [UCX Workflow](#workflows). This command is useful for developers and administrators who
want to repair a failed job. It can also be used to debug issues related to job failures. This operation can also be
done via [user interface](https://docs.databricks.com/en/workflows/jobs/repair-job-failures.html). Workflows and their
status can be listed with the [`workflows` command](#workflows-command).

[[back to top](#databricks-labs-ucx)]

## `workflows` command

See the [migration process diagram](#migration-process) to understand the role of each workflow in the migration process.

```text
$ databricks labs ucx workflows
Step                                  State    Started
assessment                            RUNNING  1 hour 2 minutes ago
099-destroy-schema                    UNKNOWN  <never run>
migrate-groups                        UNKNOWN  <never run>
remove-workspace-local-backup-groups  UNKNOWN  <never run>
validate-groups-permissions           UNKNOWN  <never run>
```

This command displays the [deployed workflows](#workflows) and their state in the current workspace. It fetches the latest
job status from the workspace and prints it in a table format. This command is useful for developers and administrators
who want to check the status of UCX workflows and ensure that they have been executed as expected. It can also be used
for debugging purposes when a workflow is not behaving as expected. Failed workflows can be fixed with
the [`repair-run` command](#repair-run-command).

[[back to top](#databricks-labs-ucx)]

## `open-remote-config` command

```commandline
databricks labs ucx open-remote-config
```

This command opens the remote configuration file in the default web browser. It generates a link to the configuration file
and opens it using the `webbrowser.open()` method. This command is useful for developers and administrators who want to view or
edit the remote configuration file without having to manually navigate to it in the workspace. It can also be used to quickly
access the configuration file from the command line. Here's the description of configuration properties:

  * `inventory_database`: A string representing the name of the inventory database.
  * `workspace_group_regex`: An optional string representing the regular expression to match workspace group names.
  * `workspace_group_replace`: An optional string to replace the matched group names with.
  * `account_group_regex`: An optional string representing the regular expression to match account group names.
  * `group_match_by_external_id`: A boolean value indicating whether to match groups by their external IDs.
  * `include_group_names`: An optional list of strings representing the names of groups to include for migration.
  * `renamed_group_prefix`: An optional string representing the prefix to add to renamed group names.
  * `instance_pool_id`: An optional string representing the ID of the instance pool.
  * `warehouse_id`: An optional string representing the ID of the warehouse.
  * `connect`: An optional `Config` object representing the configuration for connecting to the warehouse.
  * `num_threads`: An optional integer representing the number of threads to use for migration.
  * `database_to_catalog_mapping`: An optional dictionary mapping source database names to target catalog names.
  * `default_catalog`: An optional string representing the default catalog name.
  * `skip_tacl_migration`: Optional flag, allow skipping TACL migration when migrating tables or creating catalogs and schemas.
  * `default_owner_group`: Assigns this group to all migrated objects (catalogs, databases, tables, views, etc.). The group has to be an account group and the user running the migration has to be a member of this group.
  * `log_level`: An optional string representing the log level.
  * `workspace_start_path`: A string representing the starting path for notebooks and directories crawler in the workspace.
  * `instance_profile`: An optional string representing the name of the instance profile.
  * `spark_conf`: An optional dictionary of Spark configuration properties.
  * `override_clusters`: An optional dictionary mapping job cluster names to existing cluster IDs.
  * `policy_id`: An optional string representing the ID of the cluster policy.
  * `include_databases`: An optional list of strings representing the names of databases to include for migration.

[[back to top](#databricks-labs-ucx)]

## `installations` command

```text
$ databricks labs ucx installations
...
13:49:16  INFO [d.labs.ucx] Fetching installations...
13:49:17  INFO [d.l.blueprint.parallel][finding_ucx_installations_5] finding ucx installations 10/88, rps: 22.838/sec
13:49:17  INFO [d.l.blueprint.parallel][finding_ucx_installations_9] finding ucx installations 20/88, rps: 35.002/sec
13:49:17  INFO [d.l.blueprint.parallel][finding_ucx_installations_2] finding ucx installations 30/88, rps: 51.556/sec
13:49:18  INFO [d.l.blueprint.parallel][finding_ucx_installations_9] finding ucx installations 40/88, rps: 56.272/sec
13:49:18  INFO [d.l.blueprint.parallel][finding_ucx_installations_19] finding ucx installations 50/88, rps: 67.382/sec
...
Path                                      Database  Warehouse
/Users/serge.smertin@databricks.com/.ucx  ucx       675eaf1ff976aa98
```

This command displays the [installations](#installation) by different users on the same workspace. It fetches all
the installations where the `ucx` package is installed and prints their details in JSON format. This command is useful
for administrators who want to see which users have installed `ucx` and where. It can also be used to debug issues
related to multiple installations of `ucx` on the same workspace.

[[back to top](#databricks-labs-ucx)]


## `report-account-compatibility` command

```text
databricks labs ucx report-account-compatibility --profile labs-azure-account
12:56:09  INFO [databricks.sdk] Using Azure CLI authentication with AAD tokens
12:56:09  INFO [d.l.u.account.aggregate] Generating readiness report
12:56:10  INFO [databricks.sdk] Using Azure CLI authentication with AAD tokens
12:56:10  INFO [databricks.sdk] Using Azure CLI authentication with AAD tokens
12:56:15  INFO [databricks.sdk] Using Azure CLI authentication with AAD tokens
12:56:15  INFO [d.l.u.account.aggregate] Querying Schema ucx
12:56:21  WARN [d.l.u.account.aggregate] Workspace 4045495039142306 does not have UCX installed
12:56:21  INFO [d.l.u.account.aggregate] UC compatibility: 30.303030303030297% (69/99)
12:56:21  INFO [d.l.u.account.aggregate] cluster type not supported : LEGACY_TABLE_ACL: 22 objects
12:56:21  INFO [d.l.u.account.aggregate] cluster type not supported : LEGACY_SINGLE_USER: 24 objects
12:56:21  INFO [d.l.u.account.aggregate] unsupported config: spark.hadoop.javax.jdo.option.ConnectionURL: 10 objects
12:56:21  INFO [d.l.u.account.aggregate] Uses azure service principal credentials config in cluster.: 1 objects
12:56:21  INFO [d.l.u.account.aggregate] No isolation shared clusters not supported in UC: 1 objects
12:56:21  INFO [d.l.u.account.aggregate] Data is in DBFS Root: 23 objects
12:56:21  INFO [d.l.u.account.aggregate] Non-DELTA format: UNKNOWN: 5 objects
```

[[back to top](#databricks-labs-ucx)]
## `export-assessment` command

```commandline
databricks labs ucx export-assessment
```
The export-assessment command is used to export UCX assessment results to a specified location. When you run this command, you will be prompted to provide details on the destination path and the type of report you wish to generate. If you do not specify these details, the command will default to exporting the main results to the current directory. The exported file will be named based on the selection made in the format. Eg: export_{query_choice}_results.zip
- **Choose a path to save the UCX Assessment results:**
    - **Description:** Specify the path where the results should be saved. If not provided, results will be saved in the current directory.

- **Choose which assessment results to export:**
    - **Description:** Select the type of results to export. Options include:
        - `azure`
        - `estimates`
        - `interactive`
        - `main`
    - **Default:** `main`

[[back to top](#databricks-labs-ucx)]

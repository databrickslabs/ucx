# Code migration commands

See the [migration process diagram](#migration-process) to understand the role of the code migration commands in the migration process.

After you're done with the [table migration](#Table-Migration), you can proceed to the code migration.

Once you're done with the code migration, you can run the [`cluster-remap` command](#cluster-remap-command) to remap the
clusters to be UC compatible.

[[back to top](#databricks-labs-ucx)]

## `lint-local-code` command

```text
databricks labs ucx lint-local-code
```

At any time, you can run this command to assess all migrations required in a local directory or a file. It only takes seconds to run and it
gives you an initial overview of what needs to be migrated without actually performing any migration. A great way to start a migration!

This command detects all dependencies, and analyzes them. It is still experimental and at the moment only supports Python and SQL files.
We expect this command to run within a minute on code bases up to 50.000 lines of code.
Future versions of `ucx` will add support for more source types, and more migration details.

When run from an IDE terminal, this command generates output as follows:
![img.png](docs/lint-local-code-output.png)
With modern IDEs, clicking on the file link opens the file at the problematic line

[[back to top](#databricks-labs-ucx)]

## `migrate-local-code` command

```text
databricks labs ucx migrate-local-code
```

**(Experimental)** Once [table migration](#Table-Migration) is complete, you can run this command to
migrate all python and SQL files in the current working directory. This command is highly experimental and
at the moment only supports Python and SQL files and discards code comments and formatting during
the automated transformation process.

[[back to top](#databricks-labs-ucx)]

## `migrate-dbsql-dashboards` command

```text
databricks labs ucx migrate-dbsql-dashboards [--dashboard-id <dashboard-id>]
```

**(Experimental)** Once [table migration](#Table-Migration) is complete, you can run this command to
migrate all Databricks SQL dashboards in the workspace. At this moment, this command is highly experimental and discards
formatting during the automated transformation process.

This command tags dashboards & queries that have been migrated with `migrated by UCX` tag. The original queries are
also backed up in the ucx installation folder, to allow for easy rollback (see [`revert-dbsql-dashboards` command](#revert-dbsql-dashboards-command)).

This command can be run with `--dashboard-id` flag to migrate a specific dashboard.

This command is incremental and can be run multiple times to migrate new dashboards.

[[back to top](#databricks-labs-ucx)]

## `revert-dbsql-dashboards` command

```text
databricks labs ucx revert-dbsql-dashboards [--dashboard-id <dashboard-id>]
```

**(Experimental)** This command reverts the migration of Databricks SQL dashboards in the workspace, after
`migrate-dbsql-dashboards` command is executed.

This command can be run with `--dashboard-id` flag to migrate a specific dashboard.

[[back to top](#databricks-labs-ucx)]

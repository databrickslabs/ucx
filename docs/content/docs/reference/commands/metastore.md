# Metastore related commands

These commands are used to assign a Unity Catalog metastore to a workspace. The metastore assignment is a pre-requisite
for any further migration steps.

[[back to top](#databricks-labs-ucx)]

## `show-all-metastores` command

```text
databricks labs ucx show-all-metastores [--workspace-id <workspace-id>]
```

This command lists all the metastores available to be assigned to a workspace. If no workspace is specified, it lists
all the metastores available in the account. This command is useful when there are multiple metastores available within
a region, and you want to see which ones are available for assignment.

[[back to top](#databricks-labs-ucx)]

## `assign-metastore` command

```text
databricks labs ucx assign-metastore --workspace-id <workspace-id> [--metastore-id <metastore-id>]
```

This command assigns a metastore to a workspace with `--workspace-id`. If there is only a single metastore in the
workspace region, the command automatically assigns that metastore to the workspace. If there are multiple metastores
available, the command prompts for specification of the metastore (id) you want to assign to the workspace.

[[back to top](#databricks-labs-ucx)]

## `create-ucx-catalog` command

```commandline
databricks labs ucx create-ucx-catalog
16:12:59  INFO [d.l.u.hive_metastore.catalog_schema] Validating UC catalog: ucx
Please provide storage location url for catalog: ucx (default: metastore): ...
16:13:01  INFO [d.l.u.hive_metastore.catalog_schema] Creating UC catalog: ucx
```

Create and setup UCX artifact catalog. Amongst other things, the artifacts are used for tracking the migration progress
across workspaces.

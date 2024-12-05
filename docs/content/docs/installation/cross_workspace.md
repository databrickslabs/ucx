# Cross-workspace installations

When installing UCX across multiple workspaces, administrators need to keep UCX configurations in sync.
UCX will prompt you to select an account profile that has been defined in `~/.databrickscfg`. If you don't have one,
authenticate your machine with:

* `databricks auth login --host https://accounts.cloud.databricks.com/` (AWS)
* `databricks auth login --host https://accounts.azuredatabricks.net/` (Azure)

Ask your Databricks Account admin to run the [`sync-workspace-info` command](#sync-workspace-info-command) to sync the
workspace information with the UCX installations. Once the workspace information is synced, you can run the
[`create-table-mapping` command](#create-table-mapping-command) to align your tables with the Unity Catalog.

[[back to top](#databricks-labs-ucx)]

## `sync-workspace-info` command

```text
databricks --profile ACCOUNTS labs ucx sync-workspace-info
14:07:07  INFO [databricks.sdk] Using Azure CLI authentication with AAD tokens
14:07:07  INFO [d.labs.ucx] Account ID: ...
14:07:10  INFO [d.l.blueprint.parallel][finding_ucx_installations_16] finding ucx installations 10/88, rps: 16.415/sec
14:07:10  INFO [d.l.blueprint.parallel][finding_ucx_installations_0] finding ucx installations 20/88, rps: 32.110/sec
14:07:11  INFO [d.l.blueprint.parallel][finding_ucx_installations_18] finding ucx installations 30/88, rps: 39.786/sec
...
```

> Requires Databricks Account Administrator privileges. Use `--profile` to select the Databricks cli profile configured
> with access to the Databricks account console (with endpoint "https://accounts.cloud.databricks.com/"
> or "https://accounts.azuredatabricks.net").

This command uploads the workspace config to all workspaces in the account where `ucx` is installed. This command is
necessary to create an immutable default catalog mapping for [table migration](#Table-Migration) process and is the prerequisite
for [`create-table-mapping` command](#create-table-mapping-command).

If you cannot get account administrator privileges in reasonable time, you can take the risk and
run [`manual-workspace-info` command](#manual-workspace-info-command) to enter Databricks Workspace IDs and Databricks
Workspace names.

[[back to top](#databricks-labs-ucx)]

## `manual-workspace-info` command

```text
$ databricks labs ucx manual-workspace-info
14:20:36  WARN [d.l.ucx.account] You are strongly recommended to run "databricks labs ucx sync-workspace-info" by account admin,
 ... otherwise there is a significant risk of inconsistencies between different workspaces. This command will overwrite all UCX
 ... installations on this given workspace. Result may be consistent only within https://adb-987654321.10.azuredatabricks.net
Workspace name for 987654321 (default: workspace-987654321): labs-workspace
Next workspace id (default: stop): 12345
Workspace name for 12345 (default: workspace-12345): other-workspace
Next workspace id (default: stop):
14:21:19  INFO [d.l.blueprint.parallel][finding_ucx_installations_11] finding ucx installations 10/89, rps: 24.577/sec
14:21:19  INFO [d.l.blueprint.parallel][finding_ucx_installations_15] finding ucx installations 20/89, rps: 48.305/sec
...
14:21:20  INFO [d.l.ucx.account] Synchronised workspace id mapping for installations on current workspace
```

This command is only supposed to be run if the [`sync-workspace-info` command](#sync-workspace-info-command) cannot be
run. It prompts the user to enter the required information manually and creates the workspace info. This command is
useful for workspace administrators who are unable to use the `sync-workspace-info` command, because they are not
Databricks Account Administrators. It can also be used to manually create the workspace info in a new workspace.

[[back to top](#databricks-labs-ucx)]

## `create-account-groups` command

```text
$ databricks labs ucx create-account-groups [--workspace-ids 123,456,789]
```

**Requires Databricks Account Administrator privileges.** This command creates account-level groups if a workspace local
group is not present in the account. It crawls all workspaces configured in `--workspace-ids` flag, then creates
account level groups if a WS local group is not present in the account. If `--workspace-ids` flag is not specified, UCX
will create account groups for all workspaces configured in the account.

The following scenarios are supported, if a group X:
- Exist in workspaces A,B,C, and it has same members in there, it will be created in the account
- Exist in workspaces A,B but not in C, it will be created in the account
- Exist in workspaces A,B,C. It has same members in A,B, but not in C. Then, X and C_X will be created in the account

This command is useful for the setups, that don't have SCIM provisioning in place.

Once you're done with this command, proceed to the [group migration workflow](#group-migration-workflow).

[[back to top](#databricks-labs-ucx)]

## `validate-groups-membership` command

```text
$ databricks labs ucx validate-groups-membership
...
14:30:36  INFO [d.l.u.workspace_access.groups] Found 483 account groups
14:30:36  INFO [d.l.u.workspace_access.groups] No group listing provided, all matching groups will be migrated
14:30:36  INFO [d.l.u.workspace_access.groups] There are no groups with different membership between account and workspace
Workspace Group Name  Members Count  Account Group Name  Members Count  Difference
```

This command validates the groups to see if the groups at the account level and workspace level have different membership.
This command is useful for administrators who want to ensure that the groups have the correct membership. It can also be
used to debug issues related to group membership. See [group migration](docs/local-group-migration.md) and
[group migration](#group-migration-workflow) for more details.

Valid group membership is important to ensure users has correct access after legacy table ACL is migrated in [table migration process](#Table-Migration)

[[back to top](#databricks-labs-ucx)]

## `validate-table-locations` command

```text
$ databricks labs ucx validate-table-locations [--workspace-ids 123,456,789]
...
11:39:36  WARN [d.l.u.account.aggregate] Workspace 99999999 does not have UCX installed
11:39:37  WARN [d.l.u.account.aggregate] Overlapping table locations: 123456789:hive_metastore.database.table and 987654321:hive_metastore.database.table
11:39:37  WARN [d.l.u.account.aggregate] Overlapping table locations: 123456789:hive_metastore.database.table and 123456789:hive_metastore.another_database.table
```

This command validates the table locations by checking for overlapping table locations in the workspace and across
workspaces. Unity catalog does not allow overlapping table locations, also not between tables in different catalogs.
Overlapping table locations need to be resolved by the user before running the table migration.

Options to resolve tables with overlapping locations are:
- Move one table and [skip](#skip-command) the other(s).
- Duplicate the tables by copying the data into a managed table and [skip](#skip-command) the original tables.

Considerations when resolving tables with overlapping locations are:
- Migrate the tables one workspace at a time:
  - Let later migrated workspaces read tables from the earlier migrated workspace catalogs.
  - [Move](#move-command) tables between schemas and catalogs when it fits the data management model.
- The tables might have different:
  - Metadata, like:
    - Column schema (names, types, order)
    - Description
    - Tags
  - ACLs

[[back to top](#databricks-labs-ucx)]

## `cluster-remap` command

```text
$ databricks labs ucx cluster-remap
21:29:38  INFO [d.labs.ucx] Remapping the Clusters to UC
Cluster Name                                            Cluster Id
Field Eng Shared UC LTS Cluster                         0601-182128-dcbte59m
Shared Autoscaling Americas cluster                     0329-145545-rugby794
```
```text
Please provide the cluster id's as comma separated value from the above list (default: <ALL>):
```

Once you're done with the [code migration](#code-migration-commands), you can run this command to remap the clusters to UC enabled.

This command will remap the cluster to uc enabled one. When we run this command it will list all the clusters
and its id's and asks to provide the cluster id's as comma separated value which has to be remapped, by default it will take all cluster ids.
Once we provide the cluster id's it will update these clusters to UC enabled.Back up of the existing cluster
config will be stored in backup folder inside the installed location(backup/clusters/cluster_id.json) as a json file.This will help
to revert the cluster remapping.

You can revert the cluster remapping using the [`revert-cluster-remap` command](#revert-cluster-remap-command).

[[back to top](#databricks-labs-ucx)]

## `revert-cluster-remap` command

```text
$ databricks labs ucx revert-cluster-remap
21:31:29  INFO [d.labs.ucx] Reverting the Remapping of the Clusters from UC
21:31:33  INFO [d.labs.ucx] 0301-055912-4ske39iq
21:31:33  INFO [d.labs.ucx] 0306-121015-v1llqff6
Please provide the cluster id's as comma separated value from the above list (default: <ALL>):
```

If a customer want's to revert the cluster remap done using the [`cluster-remap` command](#cluster-remap-command) they can use this command to revert
its configuration from UC to original one.It will iterate through the list of clusters from the backup folder and reverts the
cluster configurations to original one.This will also ask the user to provide the list of clusters that has to be reverted as a prompt.
By default, it will revert all the clusters present in the backup folder

[[back to top](#databricks-labs-ucx)]

## `upload` command

```text
$ databricks labs ucx upload --file <file_path> --run-as-collection True
21:31:29 WARNING [d.labs.ucx] The schema of CSV files is NOT validated, ensure it is correct
21:31:29 INFO [d.labs.ucx] Finished uploading: <file_path>
```

Upload a file to a single workspace (`--run-as-collection False`) or a collection of workspaces
(`--run-as-collection True`). This command is especially useful when uploading the same file to multiple workspaces.

## `download` command

```text
$ databricks labs ucx download --file <file_path> --run-as-collection True
21:31:29 INFO [d.labs.ucx] Finished downloading: <file_path>
```

Download a csv file from a single workspace (`--run-as-collection False`) or a collection of workspaces
(`--run-as-collection True`). This command is especially useful when downloading the same file from multiple workspaces.

## `join-collection` command

```text
$ databricks labs ucx join-collection --workspace-ids <comma seperate list of workspace ids> --profile <account-profile>
```

`join-collection` command joins 2 or more workspaces into a collection. This helps in running supported cli commands as a collection
`join-collection` command updates config.yml file on each workspace ucx installation with installed_workspace_ids attribute.
In order to run `join-collectioon` command a user should:
 - be an Account admin on the Databricks account
 - be a Workspace admin on all the workspaces to be joined as a collection) or a collection of workspaces
 - have installed UCX on the workspace
The `join-collection` command will fail and throw an error msg if the above conditions are not met.

## collection eligible command

Once `join-collection` command is run, it allows user to run multiple cli commands as a collection. The following cli commands
are eligible to be run as a collection. User can run the below commands as collection by passing an additional flag `--run-as-collection=True`
- `ensure-assessment-run`
- `create-table-mapping`
- `principal-prefix-access`
- `migrate-credentials`
- `create-uber-principal`
- `create-missing-principals`
- `validate-external-location`
- `migrate-locations`
- `create-catalog-schemas`
- `migrate-tables`
- `migrate-acls`
- `migrate-dbsql-dashboards`
- `validate-group-membership`
Ex: `databricks labs ucx ensure-assessment-run --run-as-collection=True`


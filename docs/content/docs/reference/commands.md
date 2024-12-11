---
linkTitle: "Commands"
title: "Commands"
weight: 1
---
This section contains commands reference.

## Code migration commands

See the [migration process diagram](docs/process/overview.md#diagram) to understand the role of the code migration commands in the migration process.

After you're done with the [table migration](docs/reference/workflows/table_migration.md), you can proceed to the code migration.

Once you're done with the code migration, you can run the [`cluster-remap` command](docs/reference/commands.md#cluster-remap) to remap the clusters to be UC compatible.



### `lint-local-code`

```text
databricks labs ucx lint-local-code
```

At any time, you can run this command to assess all migrations required in a local directory or a file. It only takes seconds to run and it
gives you an initial overview of what needs to be migrated without actually performing any migration. A great way to start a migration!

This command detects all dependencies, and analyzes them. It is still experimental and at the moment only supports Python and SQL files.
We expect this command to run within a minute on code bases up to 50.000 lines of code.
Future versions of `ucx` will add support for more source types, and more migration details.

When run from an IDE terminal, this command generates output as follows:
![img.png](/images/lint-local-code-output.png)
With modern IDEs, clicking on the file link opens the file at the problematic line



### `migrate-local-code`

```text
databricks labs ucx migrate-local-code
```

**(Experimental)** Once [table migration](#Table-Migration) is complete, you can run this command to
migrate all python and SQL files in the current working directory. This command is highly experimental and
at the moment only supports Python and SQL files and discards code comments and formatting during
the automated transformation process.



### `migrate-dbsql-dashboards`

{{< callout type="warning">}}
**Experimental**: once [table migration](docs/reference/workflows/table_migration.md) is complete, you can run this command to
migrate all Databricks SQL dashboards in the workspace. At this moment, this command is highly experimental and discards
formatting during the automated transformation process.
{{< /callout >}}

```text
databricks labs ucx migrate-dbsql-dashboards [--dashboard-id <dashboard-id>]
```

This command tags dashboards & queries that have been migrated with `migrated by UCX` tag. The original queries are
also backed up in the ucx installation folder, to allow for easy rollback (see [`revert-dbsql-dashboards` command](#revert-dbsql-dashboards)).

This command can be run with `--dashboard-id` flag to migrate a specific dashboard.

This command is incremental and can be run multiple times to migrate new dashboards.



### `revert-dbsql-dashboards`

{{< callout type="warning">}}
**Experimental**: this command reverts the migration of Databricks SQL dashboards in the workspace, after
`migrate-dbsql-dashboards` command is executed.
{{< /callout >}}

```text
databricks labs ucx revert-dbsql-dashboards [--dashboard-id <dashboard-id>]
```



This command can be run with `--dashboard-id` flag to migrate a specific dashboard.


## Cross-workspace installations

When installing UCX across multiple workspaces, administrators need to keep UCX configurations in sync.
UCX will prompt you to select an account profile that has been defined in `~/.databrickscfg`. If you don't have one,
authenticate your machine with:

* `databricks auth login --host https://accounts.cloud.databricks.com/` (AWS)
* `databricks auth login --host https://accounts.azuredatabricks.net/` (Azure)

Ask your Databricks Account admin to run the [`sync-workspace-info` command](#sync-workspace-info) to sync the
workspace information with the UCX installations. Once the workspace information is synced, you can run the
[`create-table-mapping` command](#create-table-mapping) to align your tables with the Unity Catalog.



### `sync-workspace-info`

{{< callout type="warning" >}}
This command requires Databricks Account Administrator privileges. 

Use `--profile` to select the Databricks cli profile configured
with access to the Databricks account console (with account-level endpoint "https://accounts.cloud.databricks.com/"
or "https://accounts.azuredatabricks.net").
{{< /callout >}}

```text
databricks --profile ACCOUNTS labs ucx sync-workspace-info
14:07:07  INFO [databricks.sdk] Using Azure CLI authentication with AAD tokens
14:07:07  INFO [d.labs.ucx] Account ID: ...
14:07:10  INFO [d.l.blueprint.parallel][finding_ucx_installations_16] finding ucx installations 10/88, rps: 16.415/sec
14:07:10  INFO [d.l.blueprint.parallel][finding_ucx_installations_0] finding ucx installations 20/88, rps: 32.110/sec
14:07:11  INFO [d.l.blueprint.parallel][finding_ucx_installations_18] finding ucx installations 30/88, rps: 39.786/sec
...
```



This command uploads the workspace config to all workspaces in the account where `ucx` is installed. This command is
necessary to create an immutable default catalog mapping for [table migration](docs/process/table_migration) process and is the prerequisite
for [`create-table-mapping` command](#create-table-mapping).

If you cannot get account administrator privileges in reasonable time, you can take the risk and
run [`manual-workspace-info` command](#manual-workspace-info) to enter Databricks Workspace IDs and Databricks
Workspace names.



### `manual-workspace-info`

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

This command is only supposed to be run if the [`sync-workspace-info` command](#sync-workspace-info) cannot be
run. It prompts the user to enter the required information manually and creates the workspace info. This command is
useful for workspace administrators who are unable to use the `sync-workspace-info` command, because they are not
Databricks Account Administrators. It can also be used to manually create the workspace info in a new workspace.



### `create-account-groups`

{{< callout type="warning" >}}
This command requires Databricks Account Administrator privileges.
{{< /callout >}}

```text
$ databricks labs ucx create-account-groups [--workspace-ids 123,456,789]
```


This command creates account-level groups if a workspace local
group is not present in the account. It crawls all workspaces configured in `--workspace-ids` flag, then creates
account level groups if a WS local group is not present in the account. If `--workspace-ids` flag is not specified, UCX
will create account groups for all workspaces configured in the account.

The following scenarios are supported, if a group X:
- Exist in workspaces A,B,C, and it has same members in there, it will be created in the account
- Exist in workspaces A,B but not in C, it will be created in the account
- Exist in workspaces A,B,C. It has same members in A,B, but not in C. Then, X and C_X will be created in the account

This command is useful for the setups, that don't have SCIM provisioning in place.

Once you're done with this command, proceed to the [group migration workflow](#group-migration-workflow).



### `validate-groups-membership`

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
used to debug issues related to group membership. See [group migration](docs/dev/implementation/local-group-migration.md) and
[group migration](docs/reference/workflows/group_migration.md) for more details.

Valid group membership is important to ensure users has correct access after legacy table ACL is migrated in [table migration process](docs/process/table_migration)



### `validate-table-locations`

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
- Move one table and [skip](#skip) the other(s).
- Duplicate the tables by copying the data into a managed table and [skip](#skip) the original tables.

Considerations when resolving tables with overlapping locations are:
- Migrate the tables one workspace at a time:
  - Let later migrated workspaces read tables from the earlier migrated workspace catalogs.
  - [Move](#move) tables between schemas and catalogs when it fits the data management model.
- The tables might have different:
  - Metadata, like:
    - Column schema (names, types, order)
    - Description
    - Tags
  - ACLs



### `cluster-remap`

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

You can revert the cluster remapping using the [`revert-cluster-remap` command](#revert-cluster-remap).



### `revert-cluster-remap`

```text
$ databricks labs ucx revert-cluster-remap
21:31:29  INFO [d.labs.ucx] Reverting the Remapping of the Clusters from UC
21:31:33  INFO [d.labs.ucx] 0301-055912-4ske39iq
21:31:33  INFO [d.labs.ucx] 0306-121015-v1llqff6
Please provide the cluster id's as comma separated value from the above list (default: <ALL>):
```

If a customer want's to revert the cluster remap done using the [`cluster-remap` command](#cluster-remap) they can use this command to revert
its configuration from UC to original one.It will iterate through the list of clusters from the backup folder and reverts the
cluster configurations to original one.This will also ask the user to provide the list of clusters that has to be reverted as a prompt.
By default, it will revert all the clusters present in the backup folder



### `upload`

```text
$ databricks labs ucx upload --file <file_path> --run-as-collection True
21:31:29 WARNING [d.labs.ucx] The schema of CSV files is NOT validated, ensure it is correct
21:31:29 INFO [d.labs.ucx] Finished uploading: <file_path>
```

Upload a file to a single workspace (`--run-as-collection False`) or a collection of workspaces
(`--run-as-collection True`). This command is especially useful when uploading the same file to multiple workspaces.

### `download`

```text
$ databricks labs ucx download --file <file_path> --run-as-collection True
21:31:29 INFO [d.labs.ucx] Finished downloading: <file_path>
```

Download a csv file from a single workspace (`--run-as-collection False`) or a collection of workspaces
(`--run-as-collection True`). This command is especially useful when downloading the same file from multiple workspaces.

### `join-collection`

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

### Check collection eligibility

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

## Metastore related commands

These commands are used to assign a Unity Catalog metastore to a workspace. The metastore assignment is a pre-requisite
for any further migration steps.



### `show-all-metastores`

```text
databricks labs ucx show-all-metastores [--workspace-id <workspace-id>]
```

This command lists all the metastores available to be assigned to a workspace. If no workspace is specified, it lists
all the metastores available in the account. This command is useful when there are multiple metastores available within
a region, and you want to see which ones are available for assignment.



### `assign-metastore`

```text
databricks labs ucx assign-metastore --workspace-id <workspace-id> [--metastore-id <metastore-id>]
```

This command assigns a metastore to a workspace with `--workspace-id`. If there is only a single metastore in the
workspace region, the command automatically assigns that metastore to the workspace. If there are multiple metastores
available, the command prompts for specification of the metastore (id) you want to assign to the workspace.



### `create-ucx-catalog`

```commandline
databricks labs ucx create-ucx-catalog
16:12:59  INFO [d.l.u.hive_metastore.catalog_schema] Validating UC catalog: ucx
Please provide storage location url for catalog: ucx (default: metastore): ...
16:13:01  INFO [d.l.u.hive_metastore.catalog_schema] Creating UC catalog: ucx
```

Create and setup UCX artifact catalog. Amongst other things, the artifacts are used for tracking the migration progress
across workspaces.

## Table migration commands

These commands are vital part of [table migration process](#Table-Migration) process and require
the [assessment workflow](#assessment-workflow) and
[group migration workflow](#group-migration-workflow) to be completed.
See the [migration process diagram](#migration-process) to understand the role of the table migration commands in
the migration process.

The first step is to run the [`principal-prefix-access` command](#principal-prefix-access) to identify all
the storage accounts used by tables in the workspace and their permissions on each storage account.

If you don't have any storage credentials and external locations configured,  you'll need to run
the [`migrate-credentials` command](#migrate-credentials) to migrate the service principals
and [`migrate-locations` command](#migrate-locations) to create the external locations.
If some of the external locations already exist, you should run
the [`validate-external-locations` command](#validate-external-locations).
You'll need to create the [uber principal](#create-uber-principal) with the _**access to all storage**_ used to tables in
the workspace, so that you can migrate all the tables. If you already have the principal, you can skip this step.

Ask your Databricks Account admin to run the [`sync-workspace-info` command](#sync-workspace-info) to sync the
workspace information with the UCX installations. Once the workspace information is synced, you can run the
[`create-table-mapping` command](#create-table-mapping) to align your tables with the Unity Catalog,
[create catalogs and schemas](#create-catalogs-schemas) and start the migration using [`migrate-tables` command](#migrate-tables). During multiple runs of
the table migration workflow, you can use the [`revert-migrated-tables` command](#revert-migrated-tables) to
revert the tables that were migrated in the previous run. You can also skip the tables that you don't want to migrate
using the [`skip` command](#skip).

Once you're done with the table migration, proceed to the [code migration](#code-migration).


### `principal-prefix-access`

```text
databricks labs ucx principal-prefix-access [--subscription-ids <Azure Subscription ID>] [--aws-profile <AWS CLI profile>]
```

This command depends on results from the [assessment workflow](docs/reference/workflows/assessment.md) and requires `AWS ClI`
or `Azure CLI` to be installed and authenticated for the given machine. 

This command identifies all the storage accounts used by tables in the workspace and their permissions on each storage account.
Once you're done running this command, proceed to the [`migrate-credentials` command](#migrate-credentials).

The "prefix" refers to the start - i.e. prefix - of table locations that point to the cloud storage location.


{{< tabs items="AWS,Azure" >}}

{{< tab >}}

```commandline
databricks labs ucx principal-prefix-access --aws-profile test-profile
```

Use to identify all instance profiles in the workspace, and map their access to S3 buckets.
Also captures the IAM roles which has UC arn listed, and map their access to S3 buckets
This requires `aws` CLI to be installed and configured.

For AWS this command produces a file named `aws_instance_profile_info.csv`.
It has the following format:

| **role_arn**                                         | **resource_type** | **privilege** | **resource_path**     |
|------------------------------------------------------|-------------------|---------------|-----------------------|
| arn:aws:iam::1234:instance-profile/instance-profile1 | s3                | WRITE_FILES   | s3://s3_bucket1/path1 |

{{< /tab >}}


{{< tab >}}

```commandline
databricks labs ucx principal-prefix-access --subscription-ids test-subscription-id
```

Use to identify all storage account used by tables, identify the relevant Azure service principals and their permissions
on each storage account. The command is used to identify Azure Service Principals, which have `Storage Blob Data Contributor`,
`Storage Blob Data Reader`, `Storage Blob Data Owner` roles, or custom read/write roles on ADLS Gen2 locations that are being
used in Databricks. 
This requires Azure CLI to be installed and configured via `az login`. 

It outputs `azure_storage_account_info.csv` which will be later used by migrate-credentials command to create UC storage credentials.

{{< callout type="info" >}}
This command only lists azure storage account gen2, storage format wasb:// or adl:// are not supported in UC and those storage info
will be skipped.
{{< /callout >}}

{{< /tab >}}

{{< /tabs >}}


Once done, proceed to the [`migrate-credentials` command](#migrate-credentials).

### `create-missing-principals`

{{< callout type="info" >}}
This command is only available for AWS.
{{< /callout >}}

```bash
databricks labs ucx create-missing-principals --aws-profile <aws_profile> --single-role <single_role>
```
This command identifies all the S3 locations that are missing a UC compatible role and creates them.
It takes single-role optional parameter.
If set to True, it will create a single role for all the S3 locations.
Otherwise, it will create a role for each S3 location.

Two optional parameter are available for this command:
`--role-name` - This parameter is used to set the prefix for the role name. The default value is `UCX-ROLE`.
`--role-policy` - This parameter is used to set the prefix for the role policy name. The default value is `UCX-POLICY`.



### `delete-missing-principals`

{{< callout type="info" >}}
This command is only available for AWS.
{{< /callout >}}

```bash
databricks labs ucx delete-missing-principals --aws-profile <aws_profile>
```
This command helps to delete the IAM role created by UCX. It lists all the IAM Roles generated by the principal-prefix-access
command and allows user to select multiple roles to delete. It also checks if selected roles are mapped to any storage credentials
and asks for confirmation from user. Once confirmed, it deletes the role and its associated inline policy.



### `create-uber-principal`

{{< callout type="warning" >}}
Requires Cloud IAM admin privileges.
{{< /callout >}}

```text
databricks labs ucx create-uber-principal [--subscription-ids X]
```



Once the [`assessment` workflow](#assessment-workflow) complete, you should run this command to create a service principal with the
_**read-only access to all storage**_ used by tables in this workspace. It will also configure the
[UCX Cluster Policy](#installation) & SQL Warehouse data access configuration to use this service principal for migration
workflows. Once migration is complete, this service principal should be unprovisioned.

On Azure, it creates a principal with `Storage Blob Data Contributor` role assignment on every storage account using
Azure Resource Manager APIs.

This command is one of prerequisites for the [table migration process](#table-migration).



### `migrate-credentials`

```commandline
databricks labs ucx migrate-credentials
```

For Azure, this command prompts to confirm performing the following credential migration steps:
1. [RECOMMENDED] For each storage account, create access connectors with managed identities that have the
   `Storage Blob Data Contributor` role on the respective storage account. A storage credential is created for each
    access connector.
2. Migrate Azure Service Principals, which have `Storage Blob Data Contributor`,
   `Storage Blob Data Reader`, `Storage Blob Data Owner`, or custom roles on ADLS Gen2 locations that are being used in
   Databricks, to UC storage credentials. The Azure Service Principals to location mapping are listed
   in `/Users/{user_name}/.ucx/azure_storage_account_info.csv` which is generated by
   [`principal-prefix-access` command](#principal-prefix-access). Please review the file and delete the Service
   Principals you do not want to be migrated. The command will only migrate the Service Principals that have client
   secret stored in Databricks Secret.

  **Warning**: Service principals used to access storage accounts behind firewalls might cause connectivity issues. We
  recommend to use access connectors instead.

For AWS, this command migrates AWS Instance Profiles that are being used in Databricks, to UC storage credentials.
The AWS Instance Profiles to location mapping are listed in
{workspace ucx folder}/aws_instance_profile_info.csv which is generated by principal_prefix_access command.
Please review the file and delete the Instance Profiles you do not want to be migrated. <br/>The aws_profile parameter indicates the aws profile to use.

Once you're done with this command, run [`validate-external-locations` command](#validate-external-locations) after this one.



### `validate-external-locations`

```text
databricks labs ucx validate-external-locations
```

Once the [`assessment` workflow](docs/reference/workflows/assessment.md) finished successfully, [storage credentials](#migrate-credentials) are configured,
run this command to validate and report the missing Unity Catalog external locations to be created.

This command validates and provides mapping to external tables to external locations, also as Terraform configurations.

Once you're done with this command, proceed to the [`migrate-locations` command](#migrate-locations).



### `migrate-locations`

```text
databricks labs ucx migrate-locations
```

Once the [`assessment` workflow](docs/reference/workflows/assessment.md) finished successfully, and [storage credentials](#migrate-credentials) are configured,
run this command to have Unity Catalog external locations created. The candidate locations to be created are extracted from guess_external_locations
task in the assessment job. You can run [`validate-external-locations` command](#validate-external-locations) to check the candidate locations.

**Location ACLs:**
`migrate-locations` command applies any location ACL from existing cluster.
For Azure, it checks if there are any interactive cluster or SQL warehouse
which has service principals configured to access storage. It maps the storage account to the external location created and grants `CREATE_EXTERNAL_TABLE`,
`CREATE_EXTERNAL_VOLUME` and `READ_FILES` permission on the location to all the user who have access to the interactive cluster or SQL warehouse
For AWS, it checks any instance profiles mapped to the interactive cluster or SQL warehouse. It checks the mapping of instance profiles to the bucket. It then
maps the bucket to the external locations created and grants `CREATE_EXTERNAL_TABLE`, `CREATE_EXTERNAL_VOLUME` and `READ_FILES` permission on the location to all the user who have access to the interactive cluster
or SQL warehouse

Once you're done with this command, proceed to the [`create-table-mapping` command](#create-table-mapping).



### `create-table-mapping`

```text
databricks labs ucx create-table-mapping
```

Once the [`assessment` workflow](docs/reference/workflows/assessment.md) finished successfully and [workspace info is synchronized](#sync-workspace-info),
run this command to create the initial table mapping for review in CSV format in the Databricks Workspace:

```text
workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table
labs-azure,labs_azure,default,default,ucx_tybzs,ucx_tybzs
```

The format of the mapping file is as follows:

| **columns:** | **workspace_name**      | **catalog_name** | **src_schema** | **dst_schema** | **src_table** | **dst_table** |
|--------------|---------------------|--------------|----------------|----------------|---------------|---------------|
| values:      | data_engineering_ws | de_catalog   | database1      | database1      | table1        | table1        |

You are supposed to review this mapping and adjust it if necessary. This file is in CSV format, so that you can edit it
easier in your favorite spreadsheet application.

Once you're done with this command, [create catalogs and schemas](#create-catalogs-schemas). During
multiple runs of the table migration workflow, you can use the [`revert-migrated-tables` command](#revert-migrated-tables)
to revert the tables that were migrated in the previous run. You can also skip the tables that you don't want to migrate
using the [`skip` command](#skip).

This command is one of prerequisites for the [table migration process](docs/process/table_migration.md).

Once you're done with table migration, proceed to the [code migration](#code-migration).



### `skip`

```text
databricks labs ucx skip --schema X [--table Y] [--view Z]
```

Anytime after [`create-table-mapping` command](#create-table-mapping) is executed, you can run this command.

This command allows users to skip certain schemas, tables or views during the [table migration](#table-migration) process.
The command takes `--schema` and, optionally, `--table` and `--view` flags to specify the schema, table or view to skip.
If no `--table` flag is provided, all tables in the specified HMS database are skipped. The `--table` and `--view` can
only be used exclusively. This command is useful to temporarily disable migration of a particular schema, table or view.

Once you're done with table migration, proceed to the [code migration](#code-migration-commands).



### `unskip`

```commandline
databricks labs ucx unskip --schema X [--table Y] [--view Z]
```

This command removes the mark set by the [`skip` command](#skip) on the given schema, table or view.



### `create-catalogs-schemas`

```text
databricks labs ucx create-catalogs-schemas
```
After [`create-table-mapping` command](#create-table-mapping) is executed, you can run this command to have the required UC catalogs and schemas created.
This command is supposed to be run before migrating tables to UC using [table migration process](docs/process/table_migration.md).
Catalog & Schema ACL:
`create-catalogs-schemas` command also applies any catalog and schema ACL from existing clusters.
For Azure it checks if there are any interactive cluster or sql warehouse which has service principals configured to access storage.
It maps the storage account to the tables which has external location on those storage account created and grants `USAGE` access to
the schema and catalog if at least one such table is migrated to it.
For AWS, it checks any instance profiles mapped to the interactive cluster or sql warehouse. It checks the mapping of instance profiles
to the bucket. It then maps the bucket to the tables which has external location on those bucket created and grants `USAGE` access to
the schema and catalog if at least one such table is migrated to it.


### `migrate-tables`

```text
databricks labs ucx migrate-tables
```

Anytime after [`create-table-mapping` command](#create-table-mapping) is executed, you can run this command.

This command kicks off the [table migration](docs/process/table_migration.md) process. It triggers the `migrate-tables` workflow,
and if there are HiveSerDe tables detected, prompt whether to trigger the `migrate-external-hiveserde-tables-in-place-experimental` workflow.

Table and View ACL:
`migrate-tables` command also applies any table and view ACL from existing clusters.
For Azure it checks if there are any interactive cluster or sql warehouse which has service principals configured to access storage.
It maps the storage account to the tables which has external location on those storage account created and grants either `SELECT` permission if
the service principal only has read access on the storage account and `ALL_PRIVILEGES` if the service principal has write access on the storage account
For AWS, it checks any instance profiles mapped to the interactive cluster or sql warehouse. It checks the mapping of instance profiles
to the bucket. It then maps the bucket to the tables which has external location on those bucket created and grants either `SELECT` permission if
the instance profile only has read access on the bucket and `ALL_PRIVILEGES` if the instance profile has write access on the bucket.



### `revert-migrated-tables`

```text
databricks labs ucx revert-migrated-tables --schema X --table Y [--delete-managed]
```

Anytime after [`create-table-mapping` command](#create-table-mapping) is executed, you can run this command.

This command removes the `upgraded_from` property on a migrated table for re-migration in the [table migration](docs/process/table_migration.md) process.
This command is useful for developers and administrators who want to revert the migration of a table. It can also be used
to debug issues related to table migration.

Go back to the [`create-table-mapping` command](#create-table-mapping) after you're done with this command.



### `move`

```text
databricks labs ucx move --from-catalog A --from-schema B --from-table C --to-catalog D --to-schema E
```

This command moves a UC table/tables from one schema to another schema after
the [table migration](#Table-Migration) process. This is useful for developers and administrators who want
to adjust their catalog structure after tables upgrade.

Users will be prompted whether tables/view are dropped after moving to new schema. This only applies to `MANAGED` tables and views.

This command moves different table types differently:
- `MANAGED` tables are deep-cloned to the new schema.
- `EXTERNAL` tables are dropped from the original schema, then created in the target schema using the same location.
This is due to Unity Catalog not supporting multiple tables with overlapping paths
- `VIEW` are recreated using the same view definition.

This command supports moving multiple tables at once, by specifying `*` as the table name.



### `alias`

```text
databricks labs ucx alias --from-catalog A --from-schema B --from-table C --to-catalog D --to-schema E
```

This command aliases a UC table/tables from one schema to another schema in the same or different catalog.
It takes a `WorkspaceClient` object and `from` and `to` parameters as parameters and aliases the tables using
the `TableMove` class. This command is useful for developers and administrators who want to create an alias for a table.
It can also be used to debug issues related to table aliasing.



## Utility

### `logs`

```text
$ databricks labs ucx logs [--workflow WORKFLOW_NAME] [--debug]
```

This command displays the logs of the last run of the specified workflow. If no workflow is specified, it displays
the logs of the workflow that was run the last. This command is useful for developers and administrators who want to
check the logs of the last run of a workflow and ensure that it was executed as expected. It can also be used for
debugging purposes when a workflow is not behaving as expected. By default, only `INFO`, `WARNING`, and `ERROR` logs
are displayed. To display `DEBUG` logs, use the `--debug` flag.



### `ensure-assessment-run`

```commandline
databricks labs ucx ensure-assessment-run
```

This command ensures that the [assessment workflow](docs/reference/workflows/assessment.md) was run on a workspace.
This command will block until job finishes.
Failed workflows can be fixed with the [`repair-run` command](#repair-run). Workflows and their status can be
listed with the [`workflows` command](#workflows).



### `update-migration-progress`

```commandline
databricks labs ucx update-migration-progress
```

This command runs the [(experimental) migration progress workflow](docs/reference/workflows/progress.md) to update
the migration status of workspace resources that need to be migrated. It does this by triggering
the `migration-progress-experimental` workflow to run on a workspace and waiting for
it to complete.

Workflows and their status can be listed with the [`workflows` command](#workflows), while failed workflows can
be fixed with the [`repair-run` command](#repair-run).



### `repair-run`

```commandline
databricks labs ucx repair-run --step WORKFLOW_NAME
```

This command repairs a failed [UCX Workflow](#workflows). This command is useful for developers and administrators who
want to repair a failed job. It can also be used to debug issues related to job failures. This operation can also be
done via [user interface](https://docs.databricks.com/en/workflows/jobs/repair-job-failures.html). Workflows and their
status can be listed with the [`workflows` command](#workflows).



### `workflows`

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
the [`repair-run` command](#repair-run).



### `open-remote-config`

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
  * `log_level`: An optional string representing the log level.
  * `workspace_start_path`: A string representing the starting path for notebooks and directories crawler in the workspace.
  * `instance_profile`: An optional string representing the name of the instance profile.
  * `spark_conf`: An optional dictionary of Spark configuration properties.
  * `override_clusters`: An optional dictionary mapping job cluster names to existing cluster IDs.
  * `policy_id`: An optional string representing the ID of the cluster policy.
  * `include_databases`: An optional list of strings representing the names of databases to include for migration.



### `installations`

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




### `report-account-compatibility`

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


### `export-assessment`

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


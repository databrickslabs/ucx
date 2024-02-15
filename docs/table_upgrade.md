# Table Upgrade logic and data structures

The Hive Metastore migration process will upgrade the following Assets:

- Tables ond DBFS root
- External Tables
- Views

We don't expect this process to be a "one and done" process. This typically is an iterative process and may require a
few runs.

We suggest to keep track of the migration and provide the user a continuous feedback of the progress and status of the
upgrade.

The migration process will be set as a job that can be invoked multiple times.
Each time it will upgrade tables it can and report the ones it can't.

## Common considerations

1. One view per workspace summarizing all the table inventory and various counters
1. By default we create a single catalog per HMS (<prefix (optional)>_<workspace_name>), happens at the account level.
1. Workspace Name would be set up as part of the installation at the account level.
1. Consider other mappings of environments/database to catalog/database.
    1. The user will be able to specify a default catalog for the workspace.
1. We have to annotate the status of assets that were migrated.
1. We will roll up the migration status to the workspace/account level. Showing migration state.
1. Aggregation of migration failures
    1. View of object migration:

   | Object Type | Object ID | Migrated | Migration Failures |
      |----|----|----|----|
   |View|hive_metastore.finance.transaction_vw|1|[]|
   |Table|hive_metastore.finance.transactions|0|["Table uses SERDE: csv"]|
   |Table|hive_metastore.finance.accounts|0|[]|
   |Cluster|klasd-kladef-01265|0|["Uses Passthru authentication"]|

1. By default the target is the target_catalog/database_name
1. The assessment will generate a mapping file/table. The file will be in CSV format.

   | Source Database | Target Catalog | Target Database |
      |----|----|----|
   |finance| de_dev | finance |
   |hr | de_dev | human_resources|
   |sales | ucx-dev_ws | sales |
1. The user can download the mapping file, override the targets and upload it to the workspace .csx folder.
1. By default we copy the table content (CTAS)
1. Allow skipping individual tables/databases
1. Explore sizing tables or another threshold (recursively count bytes)
1. By default we copy the table into a managed table/managed location
1. Allow overriding target to an external table
1. We should migrate ACLs for the tables (where applicable). We should highlight cases where we can't (no direct
   translation/conflicts)
1. We should consider automating ACLs based on Instance Profiles / Service Principals and other legacy security
   mechanisms

## Tables (Parquet/Delta) on DBFS root

1. By default we copy the table content (CTAS)
1. Allow skipping individual tables/databases
1. Explore sizing tables or another threshold (recursively count bytes)
1. By default we copy the table into a managed table/managed location
1. Allow overriding target to an external table
1. Allow an exception list in case we want to skip certain tables

## Tables (Parquet/Delta) on Cloud Storage

1. Verify that we have the external locations for these tables
1. Automate creation of External Locations (Future)
1. Use sync to upgrade these tables "in place". Use the default or override catalog.database destination.
1. Update the source table with "upgraded_to" property

## Tables (None Parquet/Delta)

1. Copy this table using CTAS or Deep Clone. (Consider bringing history)
1. Copy the Metadata
1. Skip tables as needed based on size threshold or an exception list
1. Update the source table with "upgraded_to" property

## Views

1. Make a "best effort" attempt to upgrade view
1. Create a view in the new location
1. Upgrade table reference to the new tables (based on the upgraded_to table property)
1. Handle nested views
1. Handle or highlight other cases (functions/storage references/ETC)
1. Create an exception list with views failures

## Functions

1. We should migrate (if possible) functions
1. Address incompatibilities

## Account Consideration

1. HMS on multiple workspaces may point to the same assets. We need to dedupe upgrades.
1. Allow running assessment on all the accounts workspaces or on a group of workspaces.
1. We have to test on Glue and other external Metastores
1. Create an exception list at the account level the list should contain
    1. Tables that show up on more than one workspace (pointing to the same cloud storage location)
    1. Tables that show up on more than one workspace with different metadata
    1. Tables that show up on more than one workspace with different ACLs
1. Addressing table conflicts/duplications require special processing we have the following options
    1. Define a "master" and create derivative objects as views
    1. Flag and skip the dupes
    1. Duplicate the data and create dupes
1. Consider upgrading a workspace at a time. Highlight the conflict with prior upgrades.
1. Allow workspace admins to upgrade more than one workspace.

## Open Questions

1. How do we manage/surface potential cost of the assessment run in case of many workspaces.
1. How do we handle conflicts between workspaces
1. What mechanism do we use to map source to target databases
1. How to list workspaces in Azure/AWS


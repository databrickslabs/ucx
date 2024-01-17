# Assessment Report Summary

# Assessment Dashlets
## Readiness
This is a rough summary of the workspace readiness to run Unity Catalog governed workloads. Each line item is the percent of compatible items divided by the total items in the class.

## Assessment Summary
This is a summary count, per finding type of all of the findings identified during the assessment workflow. The assessment summary will help identify areas that need focus (e.g. Tables on DBFS or Clusters that need DBR upgrades)

## Database Summary
This is a Hive Metastore based Database by Database assessment summary along with an upgrade strategy.
`In Place Sync` indicates that the `SYNC` command can be used to copy the metadata into a Unity Catalog Catalog.

## External Locations
Tables were scanned for `LOCATION` attributes and that list was distilled down to External Locations. In Unity Catalog, create a STORAGE CREDENTIAL that can access the External Locations, then define UC `EXTERNAL LOCATION`s for these items.

## Mount Points
Mount points are popular means to provide access to external buckets / storage accounts. A more secure form in Unity Catalog are EXTERNAL LOCATIONs and VOLUMES. EXTERNAL LOCATIONs are the basis for EXTERNAL Tables, Schemas, Catalogs and MANAGE STORAGE. VOLUMES are the basis for managing files. The recommendation is to migrate Mountpoints to Either EXTERNAL LOCATIONS or VOLUMEs.

## Incompatible Clusters
This is a list of findings (reasons) and clusters that may need upgrading. See Assessment Finding Index (below) for specific recommendations.

## Incompatible Jobs
This is a list of findings (reasons) and jobs that may need upgrading. The reasons are often due to incompatible (non Unity Catalog) compute types. See Assessment Findings Index for more information.

## Incompatible Delta Live Tables
These are Delta Live Table jobs that may be incompatible with Unity Catalog

## Incompatible Global Init Scripts
These are Global Init Scripts that are incompatible with Unity Catalog compute. As a reminder, global init scripts need to be on secure storage (Volumes or a Cloud Storage account and not DBFS)


# Assessment Finding Index

This page will help explain UCX Assessment findings with a recommended action

### AF001 - DBR ...
Short description: The compute runtime does not meet the requirements to use Unity Catalog.
Explanation: Unity Catalog capabilities are fully enabled on Databricks Runtime 13.3 LTS. This is the current recommended runtime for production interactive clusters and jobs. This finding is noting the cluster or job compute configuration does not meet this threshold. 
recommendation: Upgrade the DBR version to 13.3 LTS or later.


### AF020 - Inplace Sync
Short description: We found that the table or database can be SYNC'd without moving data because the data is stored directly on cloud storage specified via a mount or a cloud storage URL (not DBFS).
How: Run the SYNC command on the table or schema.  If the tables (or source database) is 'managed' first set this spark setting in your session or in the interactive cluster configuration: `spark.databricks.sync.command.enableManagedTable=true`

### AF000 - Data in DBFS Root

### AF000 - unsupported config
A spark config option was found in a cluster compute definition that is incompatible with Unity Catalog based compute. The recommendation is to remove or alter the config. Additionally, Unity Catalog enabled clusters may require a different approach to the same capability. As a transition strategy, "Unassigned" clusters or "Assigned" (including job clusters but not shared clusters) may work.
- `spark.hadoop.javax.jdo.option.ConnectionURL` an external Hive Metastore is in use. Ideally migrate those tables and schemas to Unity Catalog where they can be shared across workspaces

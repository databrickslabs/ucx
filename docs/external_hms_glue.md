External Hive Metastore Integration
===

<!-- TOC -->
* [External Hive Metastore Integration](#external-hive-metastore-integration)
* [Installation](#installation)
* [Manual Override](#manual-override)
* [Assessment Workflow](#assessment-workflow)
* [Table Migration Workflow](#table-migration-workflow)
* [Additional Considerations](#additional-considerations)
<!-- TOC -->

UCX works with both the default workspace metastore, or an external Hive metastore. This document outlines the current 
integration and how to set up UCX to work with your existing external metastore.

# Installation

The setup process follows the following steps

- UCX scan existing cluster policies, and Databricks SQL data access configuration for Spark configurations key that 
enables external Hive metastore:
  - Spark config `spark.databricks.hive.metastore.glueCatalog.enabled=true` - for Glue Catalog
  - Spark config containing prefixes `spark.sql.hive.metastore` - for external Hive metastore
- If a matching cluster policy is identified, UCX prompts the user with the following message:
  _We have identified one or more cluster policies set up for an external metastore.
  Would you like to set UCX to connect to the external metastore?_
- Selecting **Yes** will display a list of the matching policies and allow the user to select the appropriate policies.
- The chosen policy will be used as the template to set up UCX job clusters via a new policy. UCX will clone the 
necessary Spark configurations and data access configurations, e.g. Instance Profile over to this new policy.
- When prompted for an inventory database, please specify a new name instead of the default `ucx` to avoid conflict.
This is because the inventory database will be created in the external metastore, which is shared across multiple workspaces.
- UCX **DOES NOT** update the data access configuration for SQL Warehouses. This is because Databricks SQL settings apply 
to all warehouses in a workspace, and can introduce unexpected changes to existing workload.

**Note**
As UCX uses both job clusters and SQL Warehouses, it is important to ensure that both are configured to use the same 
external Hive metastore. If the SQL Warehouses are not configured for external Hive metastore, please manually update
the data access configuration. See [Enable data access configuration](https://learn.microsoft.com/en-us/azure/databricks/admin/sql/data-access-configuration) for more details

[[back to top](#external-hive-metastore-integration)]

# Manual Override

If the workspace does not have a cluster policy or SQL data access configuration for external Hive metastore, there are 
two options to manually enable this:
- *Pre-installation*: create a custer policy with the appropriate Spark configuration and data access for external metastore:
  - See the following documentation pages for more details: [Glue catalog](https://docs.databricks.com/en/archive/external-metastores/aws-glue-metastore.html) and [External Hive Metastore](https://learn.microsoft.com/en-us/azure/databricks/archive/external-metastores/external-hive-metastore).
  - Below is an example cluster policy on AWS.

```json
{
  "aws_attributes.instance_profile_arn": {
    "type": "fixed",
    "value": "arn:aws:iam::999999999999:instance-profile/glue-databricks-access",
    "hidden": false
  },
  "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {
    "type": "fixed",
    "value": "true",
    "hidden": true
  }
}
```
- *Post-installation*: edit the cluster policies that are specified in the relevant UCX workflows.

If UCX does not prompt you to set up with an external metastore, you can manually override the default behavior by
following the post-installation steps above.

[[back to top](#external-hive-metastore-integration)]

# Assessment Workflow

Once UCX is set up with external Hive metastore the assessment workflow will scan tables & views from the external 
Hive metastore instead of the default workspace metastore.

If the external Hive metastore is shared between multiple workspaces, please specify a different inventory
database name for each UCX installation. This is to avoid conflicts between the inventory databases.

As the inventory database is stored in the external Hive metastore, it can only be queried from a cluster or SQL warehouse 
with external Hive metastore configuration. The assessment dashboard will also fail if the SQL warehouse is not configured correctly.

[[back to top](#external-hive-metastore-integration)]

# Table Migration Workflow

Table migration workflow will upgrade tables & views from the external Hive metastore the Unity Catalog. This workflow
only needs to be executed once per external Hive metastore. Running the workflow on other workspaces sharing the same
metastore is redundant and will be a no-op.

[[back to top](#external-hive-metastore-integration)]

# Additional Considerations

If a workspace is set up with multiple external Hive metastores, you will need to plan the approach carefully. Below are 
a few considerations to keep in mind:
- You can have multiple UCX installations in a workspace, each set up with a different external Hive metastore. As the
SQL data access configuration is shared across the entire workspace, you will need to manually update them when running
each UCX installation.
- You can uninstall UCX and reinstall it with a different external Hive metastore. This still requires manual updates to
the SQL data access configuration, but it is a cleaner approach.
- You can manually modify the cluster policy and SQL data access configuration to point to the correct external Hive 
metastore, after UCX has been installed. This is the most flexible approach, but requires manual intervention.

[[back to top](#external-hive-metastore-integration)]
# External HMS and Glue Integration

### TL;DR

The UCX toolkit by default relies on the internal workspace HMS as a source for tables and views.
<br/>The UCX is set up to run and introspect a single HMS.
<br/>The installer is looking for evidence of an external Metastore (Glue and Others)
<br/>If we find an external metastore we allow the user to use this configuration for UCX.

### Current External HMS Integration

To integrate with an External Metastore we need to configure the job clusters we generate.
<br/> The setup process follows the following steps

- We are list the existing cluster policies and look for an evidence of External Metastore
  -- Spark config `spark.databricks.hive.metastore.glueCatalog.enabled=true`
  -- Spark config containing `spark.sql.hive.metastore`
- If we find evidence of external metastore we prompt the user with the following message:<br/>
  _We have identified one or more cluster policies set up for an external metastore. <br/>
  Would you like to set UCX to connect to the external metastore._
- Selecting **Yes** will display a list of the matching policies and allow the user to select the proper one.
- We copy the Instance Profile and the spark configuration parameters from the cluster policy and apply these to the job
  clusters.
- We **DO NOT** set up the SQL Warehouse for the External HMS. If your SQL Warehouses are not configured for External
  Metastore, the Dashboard will fail.
- DBSQL Warehouse settings are global to the workspace and cannot be set individually on a single warehouse.

### Manual Setup/Override

If the workspace doesn't have a cluster policy that is set up for External Metastore, there are two options to set UCX
with External Metastore:

- Pre-installation option: create a custer policy, include the setup parameters for the External Metastore:
  -- Instance Profile (if needed, typically for Glue)
  -- Spark Configurations

```{
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

- Post installation option: edit the cluster policies in the newly created workflows. Make sure to set up the Job
  Clusters before running the workflows.
- Set up the DBSQL warehouses for the External Metastore

### Challenges and Gotchas

- UCX is currently designed to run on a single workspace at a time.
- If you run UCX on multiple workspace leveraging the same metastore, follow the following guidelines:
  -- Use a different inventory database name for each of the workspaces. Otherwise, they will override one another.
  -- Migrate the table once. Running table migration (when it will become available) from multiple workspaces is
  redundant.
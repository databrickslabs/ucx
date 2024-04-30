## Table Upgrade Guide
This document provides a guide for upgrading the Hive metastore objects to UC using UCX.

## Prerequisites
For UCX to be able to upgrade the metastore. The following prerequisites must be met:
1. UCX must be installed and configured on the workspace. For more information on how to install UCX, refer to the [UCX Readme](../README.md).
2. In case of an external metastore (such as GLUE), UCX has to be configured to attach to the metastore. For more information on how to configure UCX to attach to an external metastore, refer to the [UCX Readme](../README.md).
3. The assessment workflow must be run.
4. It is recommended that the group migration process will be completed before upgrading the metastore.

## Upgrade Process
The upgrade process is done in multiple steps. For each step we will discuss the manual process and how to perform it using UCX.

### Step 1: Mapping Metastore Tables (UCX Only)
In this step we will map the metastore tables to UC tables.
This step can be performed using the `create-table-mapping` command documented in the [UCX Readme](../README.md#create-table-mapping-command).




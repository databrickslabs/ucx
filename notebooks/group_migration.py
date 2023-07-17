# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Group Migration
# MAGIC
# MAGIC **Objective** <br/>
# MAGIC Customers who have groups created at workspace level, when they integrate with Unity Catalog and want to enable identity federation for users, groups, service principals at account level, face problems for groups federation. While users and service principals are synched up with account level identities, groups are not. As a result, customers cannot add account level groups to workspace if a workspace group with same name exists, which limits tru identity federation.
# MAGIC This notebook and the associated script is designed to help customer migrate workspace level groups to account level groups.
# MAGIC
# MAGIC **How it works** <br/>
# MAGIC The script essentially performs following major steps:
# MAGIC  - Initiate the run by providing a list of workspace group to be migrated for a given workspace
# MAGIC  - Script performs inventory of all the ACL permission for the given workspace groups
# MAGIC  - Create back up workspace group of same name but add prefix "db-temp-" and apply the same ACL on them
# MAGIC  - Delete the original workspace groups
# MAGIC  - Add account level groups to the workspace
# MAGIC  - migrate the acl from temp workspace group to the new account level groups
# MAGIC  - delete the temp workspace groups
# MAGIC  - Save the details of the inventory in a delta table
# MAGIC
# MAGIC **Scope of ACL** <br/>
# MAGIC Following objects are covered as part of the ACL migration:
# MAGIC - Clusters
# MAGIC - Cluster policies
# MAGIC - Delta Live Tables pipelines
# MAGIC - Directories
# MAGIC - Jobs
# MAGIC - MLflow experiments
# MAGIC - MLflow registered models
# MAGIC - Notebooks
# MAGIC - Files
# MAGIC - Pools
# MAGIC - Repos
# MAGIC - Databricks SQL warehouses
# MAGIC - Dashboard
# MAGIC - Query
# MAGIC - Alerts
# MAGIC - Tokens
# MAGIC - Password (for AWS)
# MAGIC - Instance Profile (for AWS)
# MAGIC - Secrets
# MAGIC - Table ACL (Non UC Cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-requisite
# MAGIC
# MAGIC Before running the script, please make sure you have the following checks
# MAGIC 1. Ensure you have equivalent account level group created for the workspace group to be migrated
# MAGIC 2. create a PAT token for the workspace which has admin access
# MAGIC 3. Ensure SCIM integration at workspace group is disabled
# MAGIC 4. Ensure no jobs or process is running the workspace using an user/service principal which is member of the workspace group
# MAGIC 5. Confirm if Table ACL is defined in the workspace and ACL defined for groups, if not Table ACL check can be skipped as it takes time to capture ACL for tables if the list is huge

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Run
# MAGIC
# MAGIC Run the script in the following sequence
# MAGIC #### Step 1: Initialize the class
# MAGIC Import the module WSGroupMigration and initialize the class by passing following attributes:
# MAGIC - list of workspace group to be migrated (make sure these are workspace groups and not account level groups)
# MAGIC - if the workspace is AWS or Azure
# MAGIC - workspace url
# MAGIC - name of the table to persist inventory data
# MAGIC - pat token of the admin to the workspace
# MAGIC - user name of the user whose pat token is generated
# MAGIC - confirm if Table ACL are used and access permission set for workspace groups

# COMMAND ----------

# MAGIC %md ## Installing the package and it's dependencies

# COMMAND ----------

from notebooks.common import install_uc_upgrade_package

install_uc_upgrade_package()

# COMMAND ----------

# MAGIC %md ## Main process entrypoint

# COMMAND ----------

# If autoGenerateList=True then groupL will be ignored and all eliglbe groups will be migrated.
autoGenerateList = False

# please provide groups here, e.g.
groupL = ["groupA", "groupB"]


# Find this in the account console
inventoryTableName = "WorkspaceInventory"

# Pull from your browser URL bar. Should start with "https://" and end with ".com" or ".net"
workspace_url = "https://<DOMAIN>"


# Personal Access Token. Create one in "User Settings"
token = "<TOKEN"

# Should the migration Check the ACL on tables/views as well?
checkTableACL = False

# What cloud provider? Acceptable values are "AWS" or anything other value.
cloud = "AWS"

# Your databricks user email.
userName = "<UserMailID>"

# Number of threads to issue Databricks API requests with. If you get a lot of errors during the inventory, lower this value.
numThreads = 30

# Initialize GroupMigrationToolkit Class with values supplied above
gm = GroupMigration(
    groupL=groupL,
    cloud=cloud,
    inventoryTableName=inventoryTableName,
    workspace_url=workspace_url,
    pat=token,
    spark=spark,
    userName=userName,
    checkTableACL=checkTableACL,
    autoGenerateList=autoGenerateList,
    numThreads=numThreads,
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Perform Dry run
# MAGIC This steps performs a dry run to verify the current ACL on the supplied workspace groups and print outs the permission.
# MAGIC Please verify if all the permissions are covered

# COMMAND ----------

gm.dryRun("Workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Create Back up group
# MAGIC This steps creates the back up groups, applies the ACL on the new temp group from the original workspace group.
# MAGIC - Verify the temp groups are created in the workspace admin console
# MAGIC - check randomly if all the ACL are applied correctly
# MAGIC - there should be one temp group for every workspace group (Ex: db-temp-analysts and analysts with same ACLs)

# COMMAND ----------

gm.createBackupGroup()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Verification: Verify backup groups
# MAGIC This steps runs the permission inventory, tracking the new temp groups
# MAGIC - Verify the temp group permissions are as seen in the initial dry run
# MAGIC - check randomly if all the ACL are applied correctly
# MAGIC - there should be one temp group for every workspace group (Ex: db-temp-analysts and analysts with same ACLs)

# COMMAND ----------

gm.dryRun("Account")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Delete original workspace group
# MAGIC This steps deletes the original workspace group.
# MAGIC - Verify original workspace groups are deleted in the workspace admin console
# MAGIC - end user permissions shouldnt be impacted as ACL permission from temp workspace group should be in effect

# COMMAND ----------

gm.deleteWorkspaceLocalGroups()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Create account level groups
# MAGIC This steps adds the account level groups to the workspace and applies the same ACL from the back workspace group to the account level group.
# MAGIC - Ensure account level groups are created upfront before
# MAGIC - verify account level groups are added to the workspace now
# MAGIC - check randomly if all the ACL are applied correctly to the account level groups
# MAGIC - there should be one temp group and account level group present (Ex: db-temp-analysts and analysts (account level group) with same ACLs)

# COMMAND ----------

gm.createAccountGroup()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6: Delete temp workspace group
# MAGIC This steps deletes the temp workspace group.
# MAGIC - Verify temp workspace groups are deleted in the workspace admin console
# MAGIC - end user permissions shouldnt be impacted as ACL permission from account level group should be in effect

# COMMAND ----------

gm.deleteTempGroups()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Complete
# MAGIC - Repeat the steps for other workspace group in the same workspace
# MAGIC - Repeat the steps for other workspace that require migration

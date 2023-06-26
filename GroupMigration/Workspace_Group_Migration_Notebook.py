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

# MAGIC %pip install tqdm
# MAGIC %pip install databricks_cli

# COMMAND ----------

HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

%load_ext autoreload
%autoreload 2

# COMMAND ----------

from WSGroupMigration import GroupMigration, loadCache, removeCache, requestGetCached

#If autoGenerateList=True then groupL will be ignored and all eliglbe groups will be migrated.
removeCacheFlag = False
autoGenerateList = True
# groupL=[<>]
groupL = [""]

#Find this in the account console
inventoryTableName="WorkspaceInventory"

#Pull from your browser URL bar. Should start with "https://" and end with ".com" or ".net"
workspace_url=HOST

#Personal Access Token. Create one in "User Settings"
token=TOKEN

#Should the migration Check the ACL on tables/views as well?
checkTableACL=False

#What cloud provider? Acceptable values are "AWS" or anything other value.
cloud='AWS'

#Your databricks user email.
# userName='<UserMailID>'
userName='amy.wang@databricks.com'

#Number of threads to issue Databricks API requests with. If you get a lot of errors during the inventory, lower this value.
numThreads = 30

#Remove cache.json
if removeCacheFlag:
    ### Removing this cache will relist all members in each group
    removeCache('cache.json')
    ### Removing this cache will relist all groups that is going to be migrated
    removeCache('cacheGroupL.json')

#Initialize GroupMigration Class with values supplied above
gm = GroupMigration( groupL = groupL , cloud=cloud , inventoryTableName = inventoryTableName, workspace_url = workspace_url, pat=token, spark=spark, userName=userName, checkTableACL = checkTableACL, autoGenerateList = autoGenerateList, numThreads=numThreads, loadDeltaCache = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Do a inventory or list of members 
# MAGIC This steps list all the members in account and workspace level group and find all the differences so you can reconcile any differences before migrating groups.

# COMMAND ----------

df = gm.groupSizeCheck()
df.display()

# COMMAND ----------

df.filter(df.ws_group_size > df.account_group_size).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Perform Dry run
# MAGIC This steps performs a dry run to verify the current ACL on the supplied workspace groups and print outs the permission.
# MAGIC Please verify if all the permissions are covered 

# COMMAND ----------

# Comment out line of object to omit from inventory listing
objListTemp = [
'Password',
'Cluster',
'ClusterPolicy',
'Warehouse',
'Dashboard',
'Query',
'Pool',
'Experiment',
'Model',
'DLT',
'Repo',
'Token',
'Secret',
'Job',
'Folder',
'Alert'
]

# TODO: (Lower Priority) Break out Folders so we skip Personal Folder
gm.dryRun("Workspace", objList=objListTemp)

# gm.persistInventory("Workspace")

# COMMAND ----------

gm.persistInventory("Workspace")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted workspaceinventory

# COMMAND ----------

# DBTITLE 1,List all Number of Assets that is associated with each Group
gm.migrationLoadPerGroup().display()

# COMMAND ----------

'''
Legal, Brand
'''
subGroupL = ['ACL:Databricks:GEN2:CAPI', "ACL:Databricks:GEN2:Proofit"]
# subGroupL = ["ACL:Databricks:GEN2:Legal"]
gm.groupL = subGroupL
gm.migrationLoadPerGroup.filter(gm.migrationLoadPerGroup.GroupName.isin(subGroupL)).display()

# COMMAND ----------

''' keep parent-child folder relations
before
access_function_engineer (ws)
  - DATA (ws) -> DATA(ac)

after
access_function_engineer (ws)
  - DATA-tmp(ws) created and added
  - DATA (ws) - removed
  - DATA (ac) - pushed to ws and added
  - DATA-tmp(ws) removed

after TF change later
access_function_engineer-ac (ac) - create new parent group done on Grammarly Side
  - DATA (ac) - added


Ideal world
before
DATA (ws) - holding all the permisssions
DATA (ac)

after option 1 (add feature to rename groups)
DATA (ws) -> DATA-qa (ws)  - holding all the permisssions
make DATA (ac) a child of DATA-qa(ws)

after option 2 (add feature to migrate groups to ac)
DATA (ws) -> replaced by DATA (ac) with all the permissions from specific ws

if we create access_function_engineer (ac) - conflict with prod access_function_engineer (ws)



workspace-prod
- access_function_engineer (ac) - entitlement - create_cluster 
  - DATA (ac)


workspace-preprod
- access_function_engineer (ac) - entitlement - create_instance_pool 
  - DATA (ac)


workspace-qa


'''

# gm.allWsLocalGroups['access_function_analyst']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Create Back up group
# MAGIC This steps creates the back up groups, applies the ACL on the new temp group from the original workspace group.
# MAGIC - Verify the temp groups are created in the workspace admin console
# MAGIC - check randomly if all the ACL are applied correctly
# MAGIC - there should be one temp group for every workspace group (Ex: db-temp-analysts and analysts with same ACLs)

# COMMAND ----------

## TODO: if fail to create temp - auto delete and retry
gm.createBackupGroupApplyPerm()

# COMMAND ----------

# # "dashboards"

# level = "Workspace"
# applyPermList = []
# for object_id,aclList in gm.dashboardPerm.items():
#   dataAcl=[]
#   for acl in aclList:
#     key = acl[0]
#     if key == 'group_name':
#       gName = acl[1]
#       permission_level = acl[2]
#     elif key == 'user_name':
#       gName = acl[1]
#       permission_level = acl[2]
#     elif key == 'service_principal_name':
#       gName = acl[1]
#       permission_level = acl[2]
  
#     if gName in gm.groupL:
#       dataAcl.insert(0,{key: "db-temp-"+gName, 'permission_level': permission_level})
    
#     dataAcl.insert(0,{key: gName, 'permission_level': permission_level})          

#   if any([str(dataAcl).count(group) > 0 for group in gm.groupL]):
#     data={"access_control_list":dataAcl}
#   print(data)

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

# gm.deleteWorkspaceLocalGroups() # debug why it try to delete Admin group
gm.bulkTryDelete(subGroupL)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Create account level groups
# MAGIC This steps adds the account level groups to the workspace and applies the same ACL from the back workspace group to the account level group.
# MAGIC - Ensure account level groups are created upfront before
# MAGIC - verify account level groups are added to the workspace now
# MAGIC - check randomly if all the ACL are applied correctly to the account level groups
# MAGIC - there should be one temp group and account level group present (Ex: db-temp-analysts and analysts (account level group) with same ACLs)

# COMMAND ----------

# MAGIC %reload_ext autoreload

# COMMAND ----------

gm.groupL = subGroupL

# COMMAND ----------

## Don't add tmpgroups arbitrarily to access function engineer.
## Check that we are only added subgroupL and not groupL
gm.createAccountGroup()

# COMMAND ----------

# gm.applyGroupPermission("Account")

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

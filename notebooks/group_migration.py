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
from databricks.sdk.runtime import dbutils  # noqa: F405

from uc_upgrade.config import (  # noqa: E402
    AccountAuthConfig,
    AuthConfig,
    GroupListingConfig,
    InventoryTableName,
    MigrationConfig,
)
from uc_upgrade.toolkits.group_migration import GroupMigrationToolkit  # noqa: E402

migration_config = MigrationConfig(
    inventory_table_name=InventoryTableName(
        catalog="main", database="default", table="uc_migration_permission_inventory"
    ),
    migrate_table_acls=False,
    auth_config=AuthConfig(
        account=AccountAuthConfig(
            # we recommend you to use Databricks Secrets to store the credentials!
            host="https://account-console-host",
            password=dbutils.secrets.get("scope", "password"),
            username=dbutils.secrets.get("scope", "username"),
            # however you can also pass the credentials directly
            # host="https://account-console-host", password="accountAdminPassword", username="accountAdminUsername"
        ),
    ),
    group_listing_config=GroupListingConfig(
        # you can provide the group list below
        # it's expected to be a list of strings with displayName values
        groups=["some_workspace_group_name"],
        # alternatively, you can automatically fetch the groups from the workspace
        # auto=True,
    ),
)

toolkit = GroupMigrationToolkit(migration_config)

# if group_listing_config.auto is True the groups will be fetched from the workspace
# if group_listing_config.auto is False, then the groups will be fetched from the group_listing_config.groups
# in both cases, group existence will be verified on the account level

toolkit.validate_groups()


# this step will clean up the inventory table if it already exists
toolkit.cleanup_inventory_table()

# this step will save all the permissions for the workspace groups in the inventory table
toolkit.inventorize_permissions()

# this step will create backup groups for the workspace groups.
# if group already exists, it will update its properties to the recent status
toolkit.create_or_update_backup_groups()


# this step will apply group permissions to the backup groups
toolkit.apply_backup_group_permissions()

# this step will delete the workspace groups and then replace them with account groups
toolkit.replace_workspace_groups_with_account_groups()

# this step will apply group permissions to the account groups
toolkit.apply_account_group_permissions()

# this step will delete the backup groups
toolkit.delete_backup_groups()

# this step will clean up the inventory table
toolkit.cleanup_inventory_table()

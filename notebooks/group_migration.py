# ruff: noqa: E501
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
# MAGIC ## Pre-requisites
# MAGIC
# MAGIC Before running the script, please make sure you have the following checks
# MAGIC 1. Ensure you have equivalent account level groups created for all workspace groups to be migrated
# MAGIC 2. Ensure no jobs or process is running the workspace using an user/service principal which is member of the workspace group
# MAGIC 3. Confirm if Table ACL is defined in the workspace and ACL defined for groups, if not Table ACL check can be skipped as it takes time to capture ACL for tables if the list is huge

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing the package and it's dependencies

# COMMAND ----------

from notebooks.common import install_uc_upgrade_package

install_uc_upgrade_package()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from databricks.sdk.runtime import dbutils  # noqa: E402

from uc_migration_toolkit.config import (  # noqa: E402
    AccountAuthConfig,
    AuthConfig,
    GroupListingConfig,
    InventoryTable,
    MigrationConfig,
)
from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit  # noqa: E402

# COMMAND ----------

# MAGIC %md ### Configuration

# COMMAND ----------

migration_config = MigrationConfig(
    inventory_table=InventoryTable(catalog="main", database="default", table="uc_migration_permission_inventory"),
    with_table_acls=False,
    auth_config=AuthConfig(
        account=AccountAuthConfig(
            # we recommend you to use Databricks Secrets to store the credentials
            host="https://account-console-host",
            username=dbutils.secrets.get("scope", "username"),
            password=dbutils.secrets.get("scope", "password"),
            account_id=dbutils.secrets.get("scope", "account_id"),
            # however, you can also pass the credentials directly
            # host="https://account-console-host",
            # username="accountAdminUsername",
            # password="accountAdminPassword",
            # account_id="account_id"
        ),
    ),
    group_listing_config=GroupListingConfig(
        # you can provide the group list below
        # it's expected to be a list of strings with displayName values
        groups=["some_group_name"],
        # alternatively, you can automatically fetch the groups from the workspace
        # auto=True,
    ),
)

toolkit = GroupMigrationToolkit(migration_config)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Migration process

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1 - Validate or fetch the groups
# MAGIC
# MAGIC
# MAGIC - If `group_listing_config.auto` is `True` the groups will be fetched from the workspace.
# MAGIC - If `group_listing_config.auto` is `False`, then the groups will be fetched from the `group_listing_config.groups` property.
# MAGIC
# MAGIC **In both cases, group existence will be verified on the workspace and account level.**

# COMMAND ----------

toolkit.validate_groups()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2 - Cleanup the inventory table
# MAGIC
# MAGIC This step will clean up the inventory table if it already exists.
# MAGIC
# MAGIC It uses the property `inventory_table` from migration config.

# COMMAND ----------

toolkit.cleanup_inventory_table()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3 - Inventorize the permissions
# MAGIC
# MAGIC This step will save all the permissions for the workspace groups in the inventory table.
# MAGIC
# MAGIC **Please note that depending on the property `with_table_acls` from config it will either collect the table ACLs, or skip them.**

# COMMAND ----------

toolkit.inventorize_permissions()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4 - Create or update backup groups
# MAGIC
# MAGIC This step will create backup groups for the workspace groups.
# MAGIC
# MAGIC Each backup group will have the same name, but with prefix, configured via the optional `backup_group_prefix` property.
# MAGIC
# MAGIC **Please note that if backup group already exists, it will update its properties to the recent status**.

# COMMAND ----------

toolkit.create_or_update_backup_groups()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 5 - apply permissions to the backup groups
# MAGIC
# MAGIC This step will apply permissions from the inventory table to the backup groups.

# COMMAND ----------

toolkit.apply_backup_group_permissions()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 6 - Replace workspace groups with account groups
# MAGIC
# MAGIC <html></br></html>
# MAGIC
# MAGIC - This step will delete the workspace groups and replace them with account-level groups.
# MAGIC - Please note that existing users shall not be affected by this change, since their permissions are granted by the backup groups we've created earlier.
# MAGIC
# MAGIC **Please note that currently we don't provide a capability to reverse this step.**
# MAGIC

# COMMAND ----------

toolkit.replace_workspace_groups_with_account_groups()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 7 - Apply group permissions to the account groups
# MAGIC
# MAGIC This step will apply the permissions from the inventory table to the account groups.

# COMMAND ----------

toolkit.apply_account_group_permissions()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 8 - Delete the backup groups
# MAGIC
# MAGIC This step will delete the backup groups.

# COMMAND ----------

toolkit.delete_backup_groups()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 9 - Cleanup the inventory table
# MAGIC
# MAGIC This step will drop the inventory table.

# COMMAND ----------

toolkit.cleanup_inventory_table()

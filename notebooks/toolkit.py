# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # UC Migration Toolkit for Groups
# MAGIC
# MAGIC
# MAGIC This notebook provides toolkit for group migration (workspace to account).
# MAGIC
# MAGIC
# MAGIC - Tested on: DBR 13.2, Single Node cluster, UC enabled (Single-User mode).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prepare imports

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from common import pip_install_dependencies

pip_install_dependencies()

# COMMAND ----------

from common import update_module_imports

update_module_imports()

# COMMAND ----------

from databricks.labs.ucx.toolkits.group_migration import GroupMigrationToolkit
from databricks.labs.ucx.config import MigrationConfig, InventoryConfig, GroupsConfig, InventoryTable, TaclConfig
from databricks.labs.ucx.toolkits.table_acls import TaclToolkit

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configuration

# COMMAND ----------

config = MigrationConfig(
    inventory=InventoryConfig(table=InventoryTable(catalog="main", database="default", name="ucx_migration_inventory")),
    groups=GroupsConfig(
        # use this option to select specific groups manually
        selected=["groupA", "groupB"],
        # use this option to select all groups automatically
        # auto=True
    ),
    tacl=TaclConfig(
        # use this option to select specific databases manually
        databases=["default"],
        # use this option to select all databases automatically
        # auto=True
    ),
    log_level="DEBUG",
)
toolkit = GroupMigrationToolkit(config)
tacltoolkit = TaclToolkit(
    toolkit._ws,
    config.inventory.table.catalog,
    config.inventory.table.schema,
    databases=config.tacl.databases,
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prepare environment
# MAGIC
# MAGIC At this step, relevant workspace-level groups will be listed, and **backup groups will be created or updated**.
# MAGIC
# MAGIC Relevant workspace groups can be either fetched automatically:
# MAGIC
# MAGIC ```
# MAGIC groups=GroupsConfig(auto=True),
# MAGIC ```
# MAGIC
# MAGIC Or manually selected:
# MAGIC
# MAGIC ```
# MAGIC groups=GroupsConfig(selected=["groupA", "groupB"]),
# MAGIC ```
# MAGIC

# COMMAND ----------

toolkit.prepare_environment()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleanup the inventory table

# COMMAND ----------

toolkit.cleanup_inventory_table()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inventorize the permissions
# MAGIC
# MAGIC Please check `README.md` for supported permissions.
# MAGIC
# MAGIC Most of the permissions are inventorized in parallel, therefore be prepared that logs might be quite verbose.

# COMMAND ----------

toolkit.inventorize_permissions()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Apply the inventorized permissions to backup groups

# COMMAND ----------

toolkit.apply_permissions_to_backup_groups()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Replace workspace-level groups with account-level groups
# MAGIC
# MAGIC *Note: only groups selected in the `prepare_environment` step will be replaced.*

# COMMAND ----------

toolkit.replace_workspace_groups_with_account_groups()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Apply the inventorized permissions to account-level groups

# COMMAND ----------

toolkit.apply_permissions_to_account_groups()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Delete the backup groups

# COMMAND ----------

toolkit.delete_backup_groups()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inventorize Table ACL's

# COMMAND ----------

tacltoolkit.grants_snapshot()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleanup the inventory table

# COMMAND ----------

toolkit.cleanup_inventory_table()

# Databricks notebook source
# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from common import pip_install_dependencies, update_module_imports

# COMMAND ----------

pip_install_dependencies()

# COMMAND ----------

from common import update_module_imports

update_module_imports()

# COMMAND ----------

from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit
from uc_migration_toolkit.config import MigrationConfig, InventoryConfig, GroupsConfig, InventoryTable

# COMMAND ----------

config = MigrationConfig(
        with_table_acls=False,
        inventory=InventoryConfig(table=InventoryTable(
            catalog="main",
            database="default",
            name="ucx_migration_inventory"
        )),
        groups=GroupsConfig(selected=["groupA", "groupB"]),
        auth=None,
        log_level="TRACE",
)
toolkit = GroupMigrationToolkit(config)

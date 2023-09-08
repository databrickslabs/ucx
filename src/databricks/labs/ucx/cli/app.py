import os
from pathlib import Path
from typing import Annotated

import typer
from typer import Typer

app = Typer(name="UC Migration Toolkit", pretty_exceptions_show_locals=True)


@app.command()
def migrate_groups(config_file: Annotated[Path, typer.Argument(help="Path to config file")] = "migration_config.yml"):
    from databricks.labs.ucx.config import MigrationConfig
    from databricks.labs.ucx.toolkits.group_migration import GroupMigrationToolkit

    config = MigrationConfig.from_file(config_file)
    toolkit = GroupMigrationToolkit(config)
    toolkit.prepare_environment()

    toolkit.cleanup_inventory_table()
    toolkit.inventorize_permissions()
    toolkit.apply_permissions_to_backup_groups()
    toolkit.replace_workspace_groups_with_account_groups()
    toolkit.apply_permissions_to_account_groups()
    toolkit.delete_backup_groups()
    toolkit.cleanup_inventory_table()


@app.command()
def generate_assessment_report():
    from databricks.sdk import WorkspaceClient

    from databricks.labs.ucx.toolkits.assessment import AssessmentToolkit

    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    catalog = os.getenv("INVENTORY_CATALOG", "ucx")
    schema = os.getenv("INVENTORY_SCHEMA", "ucx")
    ws = WorkspaceClient()
    toolkit = AssessmentToolkit(ws=ws, cluster_id=cluster_id, inventory_catalog=catalog, inventory_schema=schema)
    toolkit.compile_report()


if __name__ == "__main__":
    app()

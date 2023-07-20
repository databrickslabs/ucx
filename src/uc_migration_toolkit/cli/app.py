from pathlib import Path
from typing import Annotated

import typer
from typer import Typer

app = Typer(name="UC Migration Toolkit", pretty_exceptions_show_locals=False)


@app.command()
def migrate_groups(config_file: Annotated[Path, typer.Argument(help="Path to config file")] = "migration_config.yml"):
    from uc_migration_toolkit.cli.utils import get_migration_config
    from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit

    config = get_migration_config(config_file)
    toolkit = GroupMigrationToolkit(config)

    toolkit.validate_groups()
    toolkit.cleanup_inventory_table()
    toolkit.inventorize_permissions()
    toolkit.create_or_update_backup_groups()
    toolkit.apply_backup_group_permissions()
    toolkit.replace_workspace_groups_with_account_groups()
    toolkit.apply_account_group_permissions()
    toolkit.delete_backup_groups()
    toolkit.cleanup_inventory_table()

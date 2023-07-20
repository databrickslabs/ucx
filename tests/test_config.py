from functools import partial

import pytest

from uc_migration_toolkit.config import GroupsConfig, InventoryTable, MigrationConfig, InventoryConfig


def test_initialization():
    mc = partial(MigrationConfig,
                 inventory=InventoryConfig(table=InventoryTable(catalog="catalog", database="database", name="name")),
                 groups=GroupsConfig())

    with pytest.raises(NotImplementedError):
        mc(with_table_acls=True)

    mc(with_table_acls=False)

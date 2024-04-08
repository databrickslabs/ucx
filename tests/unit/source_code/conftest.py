from functools import partial

import pytest

from databricks.labs.ucx.hive_metastore.table_migrate import (
    MigrationStatus,
)
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.hive_metastore.tables import Table


@pytest.fixture
def empty_index():
    return MigrationIndex([])


@pytest.fixture
def migration_index():
    return MigrationIndex(
        [
            MigrationStatus('old', 'things', dst_catalog='brand', dst_schema='new', dst_table='stuff'),
            MigrationStatus('other', 'matters', dst_catalog='some', dst_schema='certain', dst_table='issues'),
        ]
    )


@pytest.fixture
def dst_lookup():
    def lookup(index, src_db: str, src_table: str) -> Table | None:
        dst = index.get(src_db, src_table)
        if dst is None:
            return None
        if not dst.dst_table or not dst.dst_catalog or not dst.dst_schema:
            return None
        return Table(dst.dst_catalog, dst.dst_schema, dst.dst_table, "type", "")

    def migration_index_lookup(index):
        return partial(lookup, index)

    return migration_index_lookup

import pytest

from databricks.labs.ucx.hive_metastore.table_migrate import (
    MigrationIndex,
    MigrationStatus,
)


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

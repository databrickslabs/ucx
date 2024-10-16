import pytest

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex, TableMigrationStatus


@pytest.fixture
def empty_index():
    return TableMigrationIndex([])


@pytest.fixture
def migration_index():
    return TableMigrationIndex(
        [
            TableMigrationStatus('old', 'things', dst_catalog='brand', dst_schema='new', dst_table='stuff'),
            TableMigrationStatus('other', 'matters', dst_catalog='some', dst_schema='certain', dst_table='issues'),
        ]
    )


@pytest.fixture
def extended_test_index():
    return TableMigrationIndex(
        [
            TableMigrationStatus('old', 'things', dst_catalog='brand', dst_schema='new', dst_table='stuff'),
            TableMigrationStatus('other', 'matters', dst_catalog='some', dst_schema='certain', dst_table='issues'),
            TableMigrationStatus('old', 'stuff', dst_catalog='brand', dst_schema='new', dst_table='things'),
            TableMigrationStatus('other', 'issues', dst_catalog='some', dst_schema='certain', dst_table='matters'),
            TableMigrationStatus(
                'default', 'testtable', dst_catalog='cata', dst_schema='nondefault', dst_table='table'
            ),
            TableMigrationStatus(
                'different_db', 'testtable', dst_catalog='cata2', dst_schema='newspace', dst_table='table'
            ),
            TableMigrationStatus('old', 'testtable', dst_catalog='cata3', dst_schema='newspace', dst_table='table'),
            TableMigrationStatus(
                'default', 'people', dst_catalog='cata4', dst_schema='nondefault', dst_table='newpeople'
            ),
            TableMigrationStatus(
                'something', 'persons', dst_catalog='cata4', dst_schema='newsomething', dst_table='persons'
            ),
            TableMigrationStatus('whatever', 'kittens', dst_catalog='cata4', dst_schema='felines', dst_table='toms'),
            TableMigrationStatus(
                'whatever', 'numbers', dst_catalog='cata4', dst_schema='counting', dst_table='numbers'
            ),
        ]
    )

import pytest

from databricks.labs.ucx.hive_metastore.migration_status import (
    MigrationStatus,
)
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex

from tests.unit import MockPathLookup


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
def extended_test_index():
    return MigrationIndex(
        [
            MigrationStatus('old', 'things', dst_catalog='brand', dst_schema='new', dst_table='stuff'),
            MigrationStatus('other', 'matters', dst_catalog='some', dst_schema='certain', dst_table='issues'),
            MigrationStatus('old', 'stuff', dst_catalog='brand', dst_schema='new', dst_table='things'),
            MigrationStatus('other', 'issues', dst_catalog='some', dst_schema='certain', dst_table='matters'),
            MigrationStatus('default', 'testtable', dst_catalog='cata', dst_schema='nondefault', dst_table='table'),
            MigrationStatus('different_db', 'testtable', dst_catalog='cata2', dst_schema='newspace', dst_table='table'),
            MigrationStatus('old', 'testtable', dst_catalog='cata3', dst_schema='newspace', dst_table='table'),
            MigrationStatus('default', 'people', dst_catalog='cata4', dst_schema='nondefault', dst_table='newpeople'),
            MigrationStatus(
                'something', 'persons', dst_catalog='cata4', dst_schema='newsomething', dst_table='persons'
            ),
            MigrationStatus('whatever', 'kittens', dst_catalog='cata4', dst_schema='felines', dst_table='toms'),
            MigrationStatus('whatever', 'numbers', dst_catalog='cata4', dst_schema='counting', dst_table='numbers'),
        ]
    )


@pytest.fixture
def mock_path_lookup() -> MockPathLookup:
    yield MockPathLookup()

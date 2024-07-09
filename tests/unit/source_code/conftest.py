import pytest

from databricks.labs.ucx.hive_metastore.migration_status import (
    MigrationStatus,
)
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.graph import DependencyResolver
from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.linters.files import ImportFileResolver, FileLoader
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader, NotebookResolver
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver


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
def simple_dependency_resolver(mock_path_lookup):
    allow_list = KnownList()
    library_resolver = PythonLibraryResolver(allow_list)
    notebook_resolver = NotebookResolver(NotebookLoader())
    import_resolver = ImportFileResolver(FileLoader(), allow_list)
    return DependencyResolver(library_resolver, notebook_resolver, import_resolver, mock_path_lookup)

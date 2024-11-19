from unittest.mock import create_autospec

from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.ownership import TableOwnership, TableOwnershipGrantLoader
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler


def test_table_ownership_grants() -> None:
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            catalog='a',
            database='b',
            name='c',
            object_type='EXTERNAL',
            table_format='PARQUET',
        ),
        Table(
            catalog='b',
            database='c',
            name='d',
            object_type='VIEW',
            table_format='...',
            view_text='...',
        ),
    ]
    table_ownership = create_autospec(TableOwnership)
    table_ownership.owner_of.return_value = 'somebody'

    loader = TableOwnershipGrantLoader(tables_crawler, table_ownership)

    grants = list(loader.load())

    assert grants == [
        Grant(
            principal='somebody',
            action_type='OWN',
            catalog='a',
            database='b',
            table='c',
        ),
        Grant(
            principal='somebody',
            action_type='OWN',
            catalog='b',
            database='c',
            view='d',
        ),
    ]

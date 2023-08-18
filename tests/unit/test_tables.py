import pytest

from uc_migration_toolkit.tacl.tables import Table


def test_is_delta_true():
    delta_table = Table(catalog='catalog', database='db', name='table', object_type='type', format='DELTA')
    assert delta_table.is_delta


def test_is_delta_false():
    non_delta_table = Table(catalog='catalog', database='db', name='table', object_type='type', format='PARQUET')
    assert not non_delta_table.is_delta


def test_key():
    table = Table(catalog='CATALOG', database='DB', name='TABLE', object_type='type', format='DELTA')
    assert table.key == 'catalog.db.table'


def test_kind_table():
    table = Table(catalog='catalog', database='db', name='table', object_type='type', format='DELTA')
    assert table.kind == 'TABLE'


def test_kind_view():
    view_table = Table(catalog='catalog', database='db', name='table', object_type='type', format='DELTA',
                       view_text='SELECT * FROM table')
    assert view_table.kind == 'VIEW'


@pytest.mark.parametrize("table,query", [
    (Table(catalog='catalog', database='db', name='managed_table', object_type='..', format='DELTA'),
     'CREATE TABLE IF NOT EXISTS new_catalog.db.managed_table DEEP CLONE '
     'hive_metastore.db.managed_table;ALTER TABLE hive_metastore.db.managed_table SET '
     "TBLPROPERTIES ('upgraded_to' = 'new_catalog.db.managed_table');"),
    (Table(catalog='catalog', database='db', name='view', object_type='..', format='DELTA',
           view_text='SELECT * FROM table'),
     'CREATE VIEW IF NOT EXISTS new_catalog.db.view AS SELECT * FROM table;'),
    (Table(catalog='catalog', database='db', name='external_table', object_type='..', format='DELTA',
           location='s3a://foo/bar'),
     'CREATE TABLE IF NOT EXISTS new_catalog.db.external_table LIKE '
     'hive_metastore.db.external_table COPY LOCATION;ALTER TABLE '
     "hive_metastore.db.external_table SET TBLPROPERTIES ('upgraded_to' = "
     "'new_catalog.db.external_table');"),
])
def test_uc_sql(table, query):
    assert table.uc_create_sql('new_catalog') == query

import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import Privilege, PrivilegeAssignment, SecurableType

from databricks.labs.ucx.hive_metastore.table_migrate import TableMove

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_move_tables_no_from_schema(ws, sql_backend, make_random, make_catalog, caplog):
    from_catalog = make_catalog()
    from_schema = make_random(4)
    to_catalog = make_catalog()
    tm = TableMove(ws, sql_backend)
    tm.move_tables(from_catalog.name, from_schema, "*", to_catalog.name, from_schema, False)
    rec_results = [
        rec.message
        for rec in caplog.records
        if f"schema {from_schema} not found in catalog {from_catalog.name}" in rec.message
    ]
    assert len(rec_results) == 1


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_move_tables(ws, sql_backend, make_catalog, make_schema, make_table, make_acc_group):
    tm = TableMove(ws, sql_backend)
    group_a = make_acc_group()
    group_b = make_acc_group()
    from_catalog = make_catalog()
    from_schema = make_schema(catalog_name=from_catalog.name)
    from_table_1 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_table_2 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_table_3 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_view_1 = make_table(
        catalog_name=from_catalog.name,
        schema_name=from_schema.name,
        view=True,
        ctas=f"select * from {from_table_2.full_name}",
    )
    to_catalog = make_catalog()
    to_schema = make_schema(catalog_name=to_catalog.name)
    # creating a table in target schema to test skipping
    to_table_3 = make_table(catalog_name=to_catalog.name, schema_name=to_schema.name, name=from_table_3.name)
    sql_backend.execute(f"GRANT SELECT ON TABLE {from_table_1.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT,MODIFY ON TABLE {from_table_2.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON VIEW {from_view_1.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {to_table_3.full_name} TO `{group_a.display_name}`")

    tm.move_tables(from_catalog.name, from_schema.name, "*", to_catalog.name, to_schema.name, False)

    to_tables = ws.tables.list(catalog_name=to_catalog.name, schema_name=to_schema.name)
    table_1_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_table_1.name}"
    )
    table_2_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_table_2.name}"
    )
    table_3_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_table_3.name}"
    )
    view_1_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_view_1.name}"
    )
    for t in to_tables:
        assert t.name in [from_table_1.name, from_table_2.name, from_table_3.name, from_view_1.name]

    expected_table_1_grant = [PrivilegeAssignment(group_a.display_name, [Privilege.SELECT])]
    expected_table_2_grant = [
        PrivilegeAssignment(group_b.display_name, [Privilege.MODIFY, Privilege.SELECT]),
    ]
    expected_table_3_grant = [PrivilegeAssignment(group_a.display_name, [Privilege.SELECT])]
    expected_view_1_grant = [PrivilegeAssignment(group_b.display_name, [Privilege.SELECT])]

    assert table_1_grant.privilege_assignments == expected_table_1_grant
    assert table_2_grant.privilege_assignments == expected_table_2_grant
    assert table_3_grant.privilege_assignments == expected_table_3_grant
    assert view_1_grant.privilege_assignments == expected_view_1_grant


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_move_tables_no_to_schema(ws, sql_backend, make_catalog, make_schema, make_table, make_random):
    tm = TableMove(ws, sql_backend)
    from_catalog = make_catalog()
    from_schema = make_schema(catalog_name=from_catalog.name)
    from_table_to_migrate = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_table_not_to_migrate = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    to_catalog = make_catalog()
    to_schema = make_random(4)

    # migrate first table
    tm.move_tables(from_catalog.name, from_schema.name, from_table_to_migrate.name, to_catalog.name, to_schema, True)

    to_tables = ws.tables.list(catalog_name=to_catalog.name, schema_name=to_schema)
    from_tables = ws.tables.list(catalog_name=from_catalog.name, schema_name=from_schema.name)

    for t in to_tables:
        assert t.name == from_table_to_migrate.name

    for t in from_tables:
        assert t.name == from_table_not_to_migrate.name


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_move_views(ws, sql_backend, make_catalog, make_schema, make_table, make_random):
    tm = TableMove(ws, sql_backend)
    from_catalog = make_catalog()
    from_schema = make_schema(catalog_name=from_catalog.name)
    from_table = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_view_to_migrate = make_table(
        catalog_name=from_catalog.name,
        schema_name=from_schema.name,
        view=True,
        ctas=f"select * from {from_table.full_name}",
    )
    from_view_not_to_migrate = make_table(
        catalog_name=from_catalog.name,
        schema_name=from_schema.name,
        view=True,
        ctas=f"select * from {from_table.full_name}",
    )
    to_catalog = make_catalog()
    to_schema = make_random(4)

    # migrate first table
    tm.move_tables(from_catalog.name, from_schema.name, from_view_to_migrate.name, to_catalog.name, to_schema, True)

    to_views = ws.tables.list(catalog_name=to_catalog.name, schema_name=to_schema)
    from_views = ws.tables.list(catalog_name=from_catalog.name, schema_name=from_schema.name)

    for t in to_views:
        assert t.name == from_view_to_migrate.name

    for t in from_views:
        assert t.name in [from_view_not_to_migrate.name, from_table.name]


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_alias_tables(ws, sql_backend, make_catalog, make_schema, make_table, make_acc_group):
    tm = TableMove(ws, sql_backend)
    group_a = make_acc_group()
    group_b = make_acc_group()
    from_catalog = make_catalog()
    from_schema = make_schema(catalog_name=from_catalog.name)
    from_table_1 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_table_2 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_table_3 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_view_1 = make_table(
        catalog_name=from_catalog.name,
        schema_name=from_schema.name,
        view=True,
        ctas=f"select * from {from_table_2.full_name} where 1=2",
    )
    to_catalog = make_catalog()
    to_schema = make_schema(catalog_name=to_catalog.name)
    # creating a table in target schema to test skipping
    to_table_3 = make_table(catalog_name=to_catalog.name, schema_name=to_schema.name, name=from_table_3.name)
    sql_backend.execute(f"GRANT SELECT ON TABLE {from_table_1.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT,MODIFY ON TABLE {from_table_2.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON VIEW {from_view_1.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {to_table_3.full_name} TO `{group_a.display_name}`")

    tm.alias_tables(from_catalog.name, from_schema.name, "*", to_catalog.name, to_schema.name)

    to_tables = ws.tables.list(catalog_name=to_catalog.name, schema_name=to_schema.name)
    table_1_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_table_1.name}"
    )
    table_2_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_table_2.name}"
    )
    table_3_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_table_3.name}"
    )
    view_1_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_view_1.name}"
    )
    for t in to_tables:
        assert t.name in [from_table_1.name, from_table_2.name, from_table_3.name, from_view_1.name]

    expected_table_1_grant = [PrivilegeAssignment(group_a.display_name, [Privilege.SELECT])]
    expected_table_2_grant = [PrivilegeAssignment(group_b.display_name, [Privilege.SELECT])]
    expected_table_3_grant = [PrivilegeAssignment(group_a.display_name, [Privilege.SELECT])]
    expected_view_1_grant = [PrivilegeAssignment(group_b.display_name, [Privilege.SELECT])]

    assert table_1_grant.privilege_assignments == expected_table_1_grant
    assert table_2_grant.privilege_assignments == expected_table_2_grant
    assert table_3_grant.privilege_assignments == expected_table_3_grant
    assert view_1_grant.privilege_assignments == expected_view_1_grant

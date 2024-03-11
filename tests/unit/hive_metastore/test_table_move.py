import logging
from unittest.mock import MagicMock, create_autospec

from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend, StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    PermissionsChange,
    PermissionsList,
    Privilege,
    PrivilegeAssignment,
    SchemaInfo,
    SecurableType,
    TableInfo,
    TableType,
)

from databricks.labs.ucx.hive_metastore.table_migrate import TableMove

logger = logging.getLogger(__name__)


def make_row(data, columns):
    row = Row(data)
    row.__columns__ = columns
    return row


def test_move_tables_invalid_from_schema(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = NotFound()
    table_move = TableMove(client, MockBackend())
    table_move.move_tables("SrcC", "SrcS", "*", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "schema SrcS not found in catalog SrcC" in rec.message]) == 1


def test_move_tables_invalid_to_schema(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    table_move = TableMove(client, MockBackend())
    table_move.move_tables("SrcC", "SrcS", "*", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "schema TgtS not found in TgtC" in rec.message]) == 1


def test_move_tables_not_found_table_error(mocker, caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
    backend.execute.side_effect = NotFound("[TABLE_OR_VIEW_NOT_FOUND]")

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
    ]
    client.tables.get.side_effect = [NotFound()]

    table_move = TableMove(client, backend)
    table_move.move_tables("SrcC", "SrcS", "table1", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "Could not find table SrcC.SrcS.table1" in rec.message]) == 1


def test_move_tables_not_found_table_unknown_error(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = create_autospec(StatementExecutionBackend)
    backend.execute.side_effect = NotFound("unknown error")

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
    ]
    client.tables.get.side_effect = [NotFound()]

    table_move = TableMove(client, backend)
    table_move.move_tables("SrcC", "SrcS", "table1", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "unknown error" in rec.message]) == 1


def test_alias_tables_not_found_table_unknown_error(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = create_autospec(StatementExecutionBackend)
    backend.execute = MagicMock()
    backend.execute.side_effect = NotFound("unknown error")

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
    ]
    client.tables.get.side_effect = [NotFound()]

    table_move = TableMove(client, backend)
    table_move.alias_tables("SrcC", "SrcS", "table1", "TgtC", "TgtS")
    assert len([rec.message for rec in caplog.records if "unknown error" in rec.message]) == 1


def test_move_tables_not_found_view_error(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = create_autospec(StatementExecutionBackend)
    backend.execute.side_effect = NotFound("[TABLE_OR_VIEW_NOT_FOUND]")

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view1",
            full_name="SrcC.SrcS.view1",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.table1",
        ),
    ]
    client.tables.get.side_effect = [NotFound()]

    table_move = TableMove(client, backend)
    table_move.move_tables("SrcC", "SrcS", "view1", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "Could not find view SrcC.SrcS.view1" in rec.message]) == 1


def test_alias_tables_not_found_view_error(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = create_autospec(StatementExecutionBackend)
    backend.execute.side_effect = NotFound("[TABLE_OR_VIEW_NOT_FOUND]")

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view1",
            full_name="SrcC.SrcS.view1",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.table1",
        ),
    ]
    client.tables.get.side_effect = [NotFound()]

    table_move = TableMove(client, backend)
    table_move.alias_tables("SrcC", "SrcS", "view1", "TgtC", "TgtS")
    assert len([rec.message for rec in caplog.records if "Could not find view SrcC.SrcS.view1" in rec.message]) == 1


def test_move_tables_not_found_view_unknown_error(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = create_autospec(StatementExecutionBackend)
    backend.execute.side_effect = NotFound("unknown error")

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view1",
            full_name="SrcC.SrcS.view1",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.table1",
        ),
    ]
    client.tables.get.side_effect = [NotFound()]

    table_move = TableMove(client, backend)
    table_move.move_tables("SrcC", "SrcS", "view1", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "unknown error" in rec.message]) == 1


def test_alias_tables_not_found_view_unknown_error(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = create_autospec(StatementExecutionBackend)
    backend.execute.side_effect = NotFound("unknown error")

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view1",
            full_name="SrcC.SrcS.view1",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.table1",
        ),
    ]
    client.tables.get.side_effect = [NotFound()]

    table_move = TableMove(client, backend)
    table_move.move_tables("SrcC", "SrcS", "view1", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "unknown error" in rec.message]) == 1


def test_move_tables_get_grants_fails_because_table_removed(caplog):
    client = create_autospec(WorkspaceClient)

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
    ]

    rows = {
        "SHOW CREATE TABLE SrcC.SrcS.table1": [
            ["CREATE TABLE SrcC.SrcS.table1 (name string)"],
        ]
    }

    client.grants.get.side_effect = NotFound('TABLE_DOES_NOT_EXIST')
    client.schemas.get.side_effect = [SchemaInfo(), SchemaInfo()]
    client.tables.get.side_effect = [NotFound(), NotFound(), NotFound(), NotFound()]
    client.grants.update = MagicMock()
    backend = MockBackend(rows=rows)
    table_move = TableMove(client, backend)
    table_move.move_tables("SrcC", "SrcS", "table1", "TgtC", "TgtS", False)

    assert "removed on the backend SrcC.SrcS.table1" in caplog.messages


def test_move_all_tables_and_drop_source():
    client = create_autospec(WorkspaceClient)

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table2",
            full_name="SrcC.SrcS.table2",
            table_type=TableType.EXTERNAL,
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table3",
            full_name="SrcC.SrcS.table3",
            table_type=TableType.MANAGED,
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view1",
            full_name="SrcC.SrcS.view1",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.table1",
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view2",
            full_name="SrcC.SrcS.view2",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.table1",
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view3",
            full_name="SrcC.SrcS.view3",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.table1",
        ),
    ]

    perm_list = PermissionsList([PrivilegeAssignment("foo", [Privilege.SELECT])])
    perm_none = PermissionsList(None)

    grants_mapping = {
        "SrcC.SrcS.table1": perm_list,
        "SrcC.SrcS.table2": perm_none,
        "SrcC.SrcS.table3": perm_none,
        "SrcC.SrcS.view1": perm_list,
        "SrcC.SrcS.view2": perm_none,
        "SrcC.SrcS.view3": perm_none,
    }

    def target_tables_mapping(full_name):
        to_migrate = ["TgtC.TgtS.table1", "TgtC.TgtS.table2", "TgtC.TgtS.view1", "TgtC.TgtS.view2"]
        if full_name in to_migrate:
            raise NotFound()

        not_to_migrate = ["TgtC.TgtS.table3", "TgtC.TgtS.view3"]
        if full_name in not_to_migrate:
            return TableInfo()
        return None

    rows = {
        "SHOW CREATE TABLE SrcC.SrcS.table1": [
            ["CREATE TABLE SrcC.SrcS.table1 (name string)"],
        ],
        "SHOW CREATE TABLE SrcC.SrcS.table2": [
            ["CREATE TABLE SrcC.SrcS.table1 (name string)"],
        ],
    }

    client.grants.get.side_effect = lambda _, full_name: grants_mapping[full_name]
    client.schemas.get.side_effect = [SchemaInfo(), SchemaInfo()]
    client.tables.get.side_effect = target_tables_mapping
    backend = MockBackend(rows=rows)
    table_move = TableMove(client, backend)
    table_move.move_tables("SrcC", "SrcS", "*", "TgtC", "TgtS", True)

    assert [
        "CREATE TABLE SrcC.SrcS.table1 (name string)",
        "CREATE TABLE TgtC.TgtS.table1 (name string)",
        "CREATE VIEW TgtC.TgtS.view1 AS SELECT * FROM SrcC.SrcS.table1",
        "CREATE VIEW TgtC.TgtS.view2 AS SELECT * FROM SrcC.SrcS.table1",
        "DROP TABLE SrcC.SrcS.table1",
        "DROP TABLE SrcC.SrcS.table2",
        "DROP VIEW SrcC.SrcS.view1",
        "DROP VIEW SrcC.SrcS.view2",
        "SHOW CREATE TABLE SrcC.SrcS.table1",
        "SHOW CREATE TABLE SrcC.SrcS.table2",
    ] == sorted(backend.queries)


def test_alias_all_tables():
    client = create_autospec(WorkspaceClient)

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table2",
            full_name="SrcC.SrcS.table2",
            table_type=TableType.EXTERNAL,
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table3",
            full_name="SrcC.SrcS.table3",
            table_type=TableType.MANAGED,
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view1",
            full_name="SrcC.SrcS.view1",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.another_table1 WHERE field1=value",
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view2",
            full_name="SrcC.SrcS.view2",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.another_table2 WHERE field2=value",
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view3",
            full_name="SrcC.SrcS.view3",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.table1",
        ),
    ]

    perm_list = PermissionsList([PrivilegeAssignment("foo", [Privilege.SELECT])])
    perm_none = PermissionsList(None)

    grants_mapping = {
        "SrcC.SrcS.table1": perm_list,
        "SrcC.SrcS.table2": perm_none,
        "SrcC.SrcS.table3": perm_none,
        "SrcC.SrcS.view1": perm_list,
        "SrcC.SrcS.view2": perm_none,
        "SrcC.SrcS.view3": perm_none,
    }

    def target_tables_mapping(full_name):
        to_migrate = ["TgtC.TgtS.table1", "TgtC.TgtS.table2", "TgtC.TgtS.view1", "TgtC.TgtS.view2"]
        if full_name in to_migrate:
            raise NotFound()

        not_to_migrate = ["TgtC.TgtS.table3", "TgtC.TgtS.view3"]
        if full_name in not_to_migrate:
            return TableInfo()
        return None

    client.grants.get.side_effect = lambda _, full_name: grants_mapping[full_name]
    client.schemas.get.side_effect = [SchemaInfo(), SchemaInfo()]
    client.tables.get.side_effect = target_tables_mapping
    backend = MockBackend()
    table_move = TableMove(client, backend)
    table_move.alias_tables("SrcC", "SrcS", "*", "TgtC", "TgtS")

    assert [
        'CREATE VIEW TgtC.TgtS.table1 AS SELECT * FROM SrcC.SrcS.table1',
        'CREATE VIEW TgtC.TgtS.table2 AS SELECT * FROM SrcC.SrcS.table2',
        'CREATE VIEW TgtC.TgtS.view1 AS SELECT * FROM SrcC.SrcS.another_table1 WHERE field1=value',
        'CREATE VIEW TgtC.TgtS.view2 AS SELECT * FROM SrcC.SrcS.another_table2 WHERE field2=value',
    ] == sorted(backend.queries)


def test_move_one_table_without_dropping_source():
    client = create_autospec(WorkspaceClient)

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table2",
            full_name="SrcC.SrcS.table2",
            table_type=TableType.EXTERNAL,
        ),
    ]

    perm_none = PermissionsList(None)

    rows = {
        "SHOW CREATE TABLE SrcC.SrcS.table1": [
            ["CREATE TABLE SrcC.SrcS.table1 (name string)"],
        ]
    }

    client.grants.get.side_effect = [perm_none, perm_none, perm_none, perm_none]
    client.schemas.get.side_effect = [SchemaInfo(), SchemaInfo()]
    client.tables.get.side_effect = [NotFound(), NotFound(), NotFound(), NotFound()]
    backend = MockBackend(rows=rows)
    table_move = TableMove(client, backend)
    table_move.move_tables("SrcC", "SrcS", "table1", "TgtC", "TgtS", False)

    assert ["CREATE TABLE TgtC.TgtS.table1 (name string)", "SHOW CREATE TABLE SrcC.SrcS.table1"] == sorted(
        backend.queries
    )


def test_move_apply_grants():
    client = create_autospec(WorkspaceClient)

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
    ]

    perm = PermissionsList([PrivilegeAssignment("user@email.com", [Privilege.SELECT, Privilege.MODIFY])])

    rows = {
        "SHOW CREATE TABLE SrcC.SrcS.table1": [
            ["CREATE TABLE SrcC.SrcS.table1 (name string)"],
        ]
    }

    client.grants.get.return_value = perm
    client.schemas.get.side_effect = [SchemaInfo(), SchemaInfo()]
    client.tables.get.side_effect = [NotFound(), NotFound(), NotFound(), NotFound()]
    client.grants.update = MagicMock()
    backend = MockBackend(rows=rows)
    table_move = TableMove(client, backend)
    table_move.move_tables("SrcC", "SrcS", "table1", "TgtC", "TgtS", False)

    assert ["CREATE TABLE TgtC.TgtS.table1 (name string)", "SHOW CREATE TABLE SrcC.SrcS.table1"] == sorted(
        backend.queries
    )
    client.grants.update.assert_called_with(
        SecurableType.TABLE,
        'TgtC.TgtS.table1',
        changes=[PermissionsChange([Privilege.SELECT, Privilege.MODIFY], "user@email.com")],
    )


def test_alias_apply_grants():
    client = create_autospec(WorkspaceClient)

    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
    ]

    perm = PermissionsList([PrivilegeAssignment("user@email.com", [Privilege.SELECT, Privilege.MODIFY])])

    rows = {
        "SHOW CREATE TABLE SrcC.SrcS.table1": [
            ["CREATE TABLE SrcC.SrcS.table1 (name string)"],
        ]
    }

    client.grants.get.return_value = perm
    client.schemas.get.side_effect = [SchemaInfo(), SchemaInfo()]
    client.tables.get.side_effect = [NotFound(), NotFound(), NotFound(), NotFound()]
    client.grants.update = MagicMock()
    backend = MockBackend(rows=rows)
    table_move = TableMove(client, backend)
    table_move.alias_tables("SrcC", "SrcS", "table1", "TgtC", "TgtS")

    assert ["CREATE VIEW TgtC.TgtS.table1 AS SELECT * FROM SrcC.SrcS.table1"] == sorted(backend.queries)
    client.grants.update.assert_called_with(
        SecurableType.TABLE, 'TgtC.TgtS.table1', changes=[PermissionsChange([Privilege.SELECT], "user@email.com")]
    )

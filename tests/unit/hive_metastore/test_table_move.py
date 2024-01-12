import logging
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    PermissionsList,
    Privilege,
    PrivilegeAssignment,
    SchemaInfo,
    TableInfo,
    TableType,
)

from databricks.labs.ucx.hive_metastore.table_migrate import TableMove
from databricks.labs.ucx.mixins.sql import Row

from ..framework.mocks import MockBackend

logger = logging.getLogger(__name__)


def make_row(data, columns):
    row = Row(data)
    row.__columns__ = columns
    return row


def test_move_tables_invalid_from_schema(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = NotFound()
    tm = TableMove(client, MockBackend)
    tm.move_tables("SrcC", "SrcS", "*", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "schema SrcS not found in catalog SrcC" in rec.message]) == 1


def test_move_tables_invalid_to_schema(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    tm = TableMove(client, MockBackend)
    tm.move_tables("SrcC", "SrcS", "*", "TgtC", "TgtS", False)
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

    tm = TableMove(client, backend)
    tm.move_tables("SrcC", "SrcS", "table1", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "Could not find table SrcC.SrcS.table1" in rec.message]) == 1


def test_move_tables_not_found_table_unknown_error(mocker, caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
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

    tm = TableMove(client, backend)
    tm.move_tables("SrcC", "SrcS", "table1", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "unknown error" in rec.message]) == 1


def test_move_tables_not_found_view_error(mocker, caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
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

    tm = TableMove(client, backend)
    tm.move_tables("SrcC", "SrcS", "view1", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "Could not find view SrcC.SrcS.view1" in rec.message]) == 1


def test_move_tables_not_found_view_unknown_error(mocker, caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    backend = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
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

    tm = TableMove(client, backend)
    tm.move_tables("SrcC", "SrcS", "view1", "TgtC", "TgtS", False)
    assert len([rec.message for rec in caplog.records if "unknown error" in rec.message]) == 1


def test_move_all_tables(caplog):
    caplog.set_level(logging.INFO)
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
    tm = TableMove(client, backend)
    tm.move_tables("SrcC", "SrcS", "*", "TgtC", "TgtS", True)

    expected_messages = [
        "Moved 2 tables to the new schema TgtS.",
        "Moved 2 views to the new schema TgtS.",
        "Creating table TgtC.TgtS.table1",
        "Applying grants on table TgtC.TgtS.table1",
        "Dropping source table SrcC.SrcS.table1",
        "Creating table TgtC.TgtS.table2",
        "Dropping source table SrcC.SrcS.table2",
        "Creating view TgtC.TgtS.view1",
        "Applying grants on view TgtC.TgtS.view1",
        "Dropping source view SrcC.SrcS.view1",
        "Creating view TgtC.TgtS.view2",
        "Dropping source view SrcC.SrcS.view2",
    ]

    log_count = 0
    for rec in caplog.records:
        print(rec)
        if rec.message in expected_messages:
            log_count += 1

    assert log_count == len(expected_messages)

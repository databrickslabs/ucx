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
    tm.move_tables("SrcC", "SrcS", "*", "TgtC", "TgtS")
    assert len([rec.message for rec in caplog.records if "schema SrcS not found in catalog SrcC" in rec.message]) == 1


def test_move_tables_invalid_to_schema(caplog):
    client = create_autospec(WorkspaceClient)
    client.schemas.get.side_effect = [SchemaInfo(), NotFound()]
    tm = TableMove(client, MockBackend)
    tm.move_tables("SrcC", "SrcS", "*", "TgtC", "TgtS")
    assert len([rec.message for rec in caplog.records if "schema TgtS not found in TgtC" in rec.message]) == 1


def test_move_tables(caplog):
    caplog.set_level(logging.INFO)
    client = create_autospec(WorkspaceClient)
    errors = {}
    rows = {
        "SHOW CREATE TABLE SrcC.SrcS.table1": [
            ("CREATE TABLE SrcC.SrcS.table1 (name string)"),
        ],
        "SHOW CREATE TABLE SrcC.SrcS.table3": [
            ("CREATE TABLE SrcC.SrcS.table3 (name string)"),
        ],
    }
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
            table_type=TableType.EXTERNAL,
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
    ]
    perm_list = PermissionsList([PrivilegeAssignment("foo", [Privilege.SELECT])])
    perm_none = PermissionsList(None)
    client.grants.get.side_effect = [perm_list, perm_none, perm_none, perm_list, perm_none]
    client.schemas.get.side_effect = [SchemaInfo(), SchemaInfo()]
    client.tables.get.side_effect = [NotFound(), TableInfo(), NotFound(), NotFound(), NotFound()]
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tm = TableMove(client, backend)
    tm.move_tables("SrcC", "SrcS", "*", "TgtC", "TgtS")
    log_cnt = 0
    for rec in caplog.records:
        if rec.message in ["migrated 2 tables to the new schema TgtS.", "migrated 2 views to the new schema TgtS."]:
            log_cnt += 1

    assert log_cnt == 2

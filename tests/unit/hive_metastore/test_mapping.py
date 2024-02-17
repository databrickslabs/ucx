import io
from unittest.mock import MagicMock, call, create_autospec

import pytest
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.parallel import ManyError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import TableInfo

from databricks.labs.ucx.account import WorkspaceInfo
from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.hive_metastore.mapping import (
    Rule,
    TableMapping,
    TableToMigrate,
)
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler

from ..framework.mocks import MockBackend

MANAGED_DELTA_TABLE = Table(
    object_type="MANAGED",
    table_format="DELTA",
    catalog="hive_metastore",
    database="test_schema1",
    name="test_table2",
)

VIEW = Table(
    object_type="VIEW",
    table_format="VIEW",
    catalog="hive_metastore",
    database="test_schema1",
    name="test_view1",
    view_text="SELECT * FROM SOMETHING",
    upgraded_to="cat1.schema1.dest_view1",
)

EXTERNAL_DELTA_TABLE = Table(
    object_type="EXTERNAL",
    table_format="DELTA",
    catalog="hive_metastore",
    database="test_schema1",
    name="test_table1",
    upgraded_to="cat1.schema1.dest1",
)


def test_current_tables_empty_fails():
    ws = MagicMock()
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    installation = MockInstallation()
    table_mapping = TableMapping(installation, ws, backend)

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = []

    with pytest.raises(ValueError):
        list(table_mapping.current_tables(tables_crawler, "a", "b"))


def test_current_tables_some_rules():
    ws = MagicMock()
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    installation = MockInstallation()
    table_mapping = TableMapping(installation, ws, backend)

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            catalog="hive_metastore",
            object_type="TABLE",
            table_format="DELTA",
            database="foo",
            name="bar",
        )
    ]

    rule = next(table_mapping.current_tables(tables_crawler, "a", "b"))

    assert rule == Rule(
        workspace_name="a", catalog_name="b", src_schema="foo", dst_schema="foo", src_table="bar", dst_table="bar"
    )
    assert rule.as_uc_table_key == "b.foo.bar"
    assert rule.as_hms_table_key == "hive_metastore.foo.bar"


def test_save_mapping():
    ws = MagicMock()
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    installation = MockInstallation()
    table_mapping = TableMapping(installation, ws, backend)

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            catalog="hive_metastore",
            object_type="TABLE",
            table_format="DELTA",
            database="foo",
            name="bar",
        )
    ]

    workspace_info = create_autospec(WorkspaceInfo)
    workspace_info.current.return_value = "foo-bar"

    table_mapping.save(tables_crawler, workspace_info)

    installation.assert_file_written(
        'mapping.csv',
        [
            {
                'catalog_name': 'foo_bar',
                'dst_schema': 'foo',
                'dst_table': 'bar',
                'src_schema': 'foo',
                'src_table': 'bar',
                'workspace_name': 'foo-bar',
            }
        ],
    )


def test_load_mapping_not_found():
    ws = MagicMock()
    ws.workspace.download.side_effect = NotFound(...)
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    installation = MockInstallation()
    table_mapping = TableMapping(installation, ws, backend)

    with pytest.raises(ValueError):
        table_mapping.load()


def test_load_mapping():
    ws = create_autospec(WorkspaceClient)
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    installation = MockInstallation(
        {
            'mapping.csv': [
                {
                    'catalog_name': 'foo_bar',
                    'dst_schema': 'foo',
                    'dst_table': 'bar',
                    'src_schema': 'foo',
                    'src_table': 'bar',
                    'workspace_name': 'foo-bar',
                }
            ]
        }
    )
    table_mapping = TableMapping(installation, ws, backend)

    rules = table_mapping.load()

    assert [
        Rule(
            workspace_name="foo-bar",
            catalog_name="foo_bar",
            src_schema="foo",
            dst_schema="foo",
            src_table="bar",
            dst_table="bar",
        )
    ] == rules


def test_skip_happy_path(caplog):
    ws = create_autospec(WorkspaceClient)
    sbe = create_autospec(SqlBackend)
    installation = MockInstallation()
    mapping = TableMapping(installation, ws, sbe)
    mapping.skip_table(schema="schema", table="table")
    sbe.execute.assert_called_with(f"ALTER TABLE schema.table SET TBLPROPERTIES('{mapping.UCX_SKIP_PROPERTY}' = true)")
    assert len(caplog.records) == 0
    mapping.skip_schema(schema="schema")
    sbe.execute.assert_called_with(f"ALTER SCHEMA schema SET DBPROPERTIES('{mapping.UCX_SKIP_PROPERTY}' = true)")
    assert len(caplog.records) == 0


def test_skip_missing_schema(caplog):
    ws = create_autospec(WorkspaceClient)
    sbe = create_autospec(SqlBackend)
    installation = MockInstallation()
    sbe.execute.side_effect = NotFound("[SCHEMA_NOT_FOUND]")
    mapping = TableMapping(installation, ws, sbe)
    mapping.skip_schema(schema="schema")
    assert [rec.message for rec in caplog.records if "schema not found" in rec.message.lower()]


def test_skip_missing_table(caplog):
    ws = create_autospec(WorkspaceClient)
    sbe = create_autospec(SqlBackend)
    installation = MockInstallation()
    sbe.execute.side_effect = NotFound("[TABLE_OR_VIEW_NOT_FOUND]")
    mapping = TableMapping(installation, ws, sbe)
    mapping.skip_table('foo', table="table")
    assert [rec.message for rec in caplog.records if "table not found" in rec.message.lower()]


def test_extract_database_skip_property():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "ucx")
    assert "databricks.labs.ucx.skip" in table_crawler.parse_database_props("(databricks.labs.ucx.skip,true)")


def test_skip_tables_marked_for_skipping_or_upgraded():
    errors = {}
    rows = {
        "SHOW DATABASES": [
            ["test_schema1"],
            ["test_schema2"],
            ["test_schema3"],
        ],
        "SHOW TBLPROPERTIES test_schema1.test_table1": [
            {"key": "upgraded_to", "value": "fake_dest"},
        ],
        "SHOW TBLPROPERTIES test_schema1.test_view1": [
            {"key": "databricks.labs.ucx.skip", "value": "true"},
        ],
        "SHOW TBLPROPERTIES test_schema1.test_table2": [
            {"key": "upgraded_to", "value": "fake_dest"},
        ],
        "DESCRIBE SCHEMA EXTENDED test_schema1": [],
        "DESCRIBE SCHEMA EXTENDED test_schema2": [],
        "DESCRIBE SCHEMA EXTENDED test_schema3": [
            {
                "database_description_item": "Properties",
                "database_description_value": "((databricks.labs.ucx.skip,true))",
            },
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    client.tables.get.side_effect = [
        TableInfo(
            catalog_name="cat1",
            schema_name="schema1",
            name="dest1",
            full_name="cat1.schema1.test_table1",
            properties={"upgraded_from": "hive_metastore.test_schema1.test_table1"},
        ),
        NotFound(),
        NotFound(),
        NotFound(),
        NotFound(),
    ]

    test_tables = [
        EXTERNAL_DELTA_TABLE,
        VIEW,
        MANAGED_DELTA_TABLE,
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema2",
            name="test_table3",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema3",
            name="test_table4",
        ),
    ]
    client.workspace.download.return_value = io.BytesIO(
        (
            "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
            "foo-bar,cat1,test_schema1,schema1,test_table1,test_table1\r\n"
            "foo-bar,cat1,test_schema1,schema1,test_view1,test_view1\r\n"
            "foo-bar,cat1,test_schema1,schema1,test_table2,test_table2\r\n"
            "foo-bar,cat1,test_schema2,schema2,test_table3,test_table3\r\n"
            "foo-bar,cat1,test_schema3,schema3,test_table4,test_table4\r\n"
        ).encode("utf8")
    )
    table_crawler.snapshot.return_value = test_tables
    installation = Installation(client, "ucx")
    table_mapping = TableMapping(installation, client, backend)

    tables_to_migrate = table_mapping.get_tables_to_migrate(table_crawler)
    assert len(tables_to_migrate) == 2
    tables = (table_to_migrate.src for table_to_migrate in tables_to_migrate)
    assert (
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema3",
            name="test_table4",
        )
        not in tables
    )
    assert (table for table in tables_to_migrate)
    assert call("fake_dest") in client.tables.get.call_args_list


def test_table_with_no_target_reverted():
    errors = {}
    rows = {
        "SHOW TBLPROPERTIES schema1.table1": [
            {"key": "upgraded_to", "value": "non.existing.table"},
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    client = create_autospec(WorkspaceClient)
    client.tables.get.side_effect = NotFound()

    installation = Installation(client, "ucx")
    table_mapping = TableMapping(installation, client, backend)
    table_to_migrate = Table(
        object_type="EXTERNAL",
        table_format="DELTA",
        catalog="hive_metastore",
        database="schema1",
        name="table1",
    )
    rule = Rule("fake_ws", "cat1", "schema1", "schema1", "table1", "table1")
    assert table_mapping._get_table_in_scope_task(TableToMigrate(table_to_migrate, rule))
    assert "ALTER TABLE hive_metastore.schema1.table1 UNSET TBLPROPERTIES IF EXISTS('upgraded_to');" in backend.queries


def test_skipping_rules_existing_targets():
    client = create_autospec(WorkspaceClient)
    client.workspace.download.return_value = io.BytesIO(
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
        "fake_ws,cat1,schema1,schema1,table1,dest1\r\n".encode("utf8")
    )
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)

    client.tables.get.return_value = TableInfo(
        catalog_name="cat1",
        schema_name="schema1",
        name="dest1",
        full_name="cat1.schema1.test_table1",
        properties={"upgraded_from": "hive_metastore.schema1.table1"},
    )

    installation = Installation(client, "ucx")
    table_mapping = TableMapping(installation, client, backend)
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table1",
        ),
    ]
    table_mapping.get_tables_to_migrate(tables_crawler)

    assert ["DESCRIBE SCHEMA EXTENDED schema1"] == backend.queries


def test_mismatch_from_table_raises_exception():
    client = create_autospec(WorkspaceClient)
    client.workspace.download.return_value = io.BytesIO(
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
        "fake_ws,cat1,schema1,schema1,table1,dest1\r\n".encode("utf8")
    )
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)

    client.tables.get.return_value = TableInfo(
        catalog_name="cat1",
        schema_name="schema1",
        name="dest1",
        full_name="cat1.schema1.test_table1",
        properties={"upgraded_from": "hive_metastore.schema1.bad_table"},
    )

    installation = Installation(client, "ucx")
    table_mapping = TableMapping(installation, client, backend)
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table1",
        ),
    ]
    with pytest.raises(ManyError, match="ResourceConflict"):
        table_mapping.get_tables_to_migrate(tables_crawler)

    assert ["DESCRIBE SCHEMA EXTENDED schema1"] == backend.queries


def test_table_not_in_crawled_tables():
    client = create_autospec(WorkspaceClient)
    client.workspace.download.return_value = io.BytesIO(
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
        "fake_ws,cat1,schema1,schema1,table1,dest1\r\n".encode("utf8")
    )
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    installation = Installation(client, "ucx")
    table_mapping = TableMapping(installation, client, backend)
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = []
    table_mapping.get_tables_to_migrate(tables_crawler)

    assert ["DESCRIBE SCHEMA EXTENDED schema1"] == backend.queries


def test_skipping_rules_database_skipped():
    client = MagicMock()
    client.workspace.download.return_value = io.BytesIO(
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
        "fake_ws,cat1,schema1,schema1,table1,dest1\r\n"
        "fake_ws,cat1,schema2,schema2,table2,dest2\r\n".encode("utf8")
    )
    errors = {}
    rows = {
        "DESCRIBE SCHEMA EXTENDED schema2": [
            {
                "database_description_item": "Properties",
                "database_description_value": "((databricks.labs.ucx.skip,true))",
            },
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    client.tables.get.side_effect = NotFound()
    client.catalogs.list.return_value = []
    client.schemas.list.return_value = []
    client.tables.list.return_value = []

    installation = Installation(client, "ucx")
    table_mapping = TableMapping(installation, client, backend)
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table1",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema2",
            name="table2",
        ),
    ]
    table_mapping.get_tables_to_migrate(tables_crawler)

    assert "SHOW TBLPROPERTIES schema1.table1" in backend.queries
    assert "SHOW TBLPROPERTIES schema2.table2" not in backend.queries


def test_skip_missing_table_in_snapshot():
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table1",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema2",
            name="table2",
        ),
    ]

    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    client = create_autospec(WorkspaceClient)
    client.tables.get.side_effect = NotFound()
    client.catalogs.list.return_value = []
    client.schemas.list.return_value = []
    client.tables.list.return_value = []

    installation = MockInstallation({'mapping.csv': []})
    table_mapping = TableMapping(installation, client, backend)
    table_mapping.get_tables_to_migrate(tables_crawler)

    assert not backend.queries


def test_skipping_rules_target_exists():
    client = MagicMock()
    client.workspace.download.return_value = io.BytesIO(
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
        "fake_ws,cat1,schema2,schema2,table2,dest2\r\n"
        "fake_ws,cat1,schema2,schema2,table2,dest2\r\n".encode("utf8")
    )
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table1",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema2",
            name="table2",
        ),
    ]

    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    client.catalogs.list.return_value = []
    client.schemas.list.return_value = []
    client.tables.list.return_value = []
    client.tables.get.side_effect = [
        NotFound(),
        TableInfo(
            catalog_name="cat1",
            schema_name="schema2",
            name="dest2",
            full_name="cat1.schema2.dest2",
            properties={},
        ),
    ]

    installation = Installation(client, "ucx")
    table_mapping = TableMapping(installation, client, backend)

    assert len(table_mapping.get_tables_to_migrate(tables_crawler)) == 1


def test_is_target_exists():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    client = create_autospec(WorkspaceClient)
    client.catalogs.list.return_value = []
    client.schemas.list.return_value = []
    client.tables.list.return_value = []
    client.tables.get.side_effect = [
        NotFound(),
        TableInfo(
            catalog_name="cat1",
            schema_name="schema2",
            name="dest2",
            full_name="cat1.schema2.dest2",
            properties={},
        ),
    ]

    installation = Installation(client, "ucx")
    table_mapping = TableMapping(installation, client, backend)

    src_table = Table(
        catalog="hive_metastore", database="schema1", name="dest1", object_type="MANAGED", table_format="DELTA"
    )
    assert not table_mapping._exists_in_uc(src_table, "cat1.schema1.dest1")
    assert table_mapping._exists_in_uc(src_table, "cat1.schema2.dest2")

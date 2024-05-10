import io
from unittest.mock import call, create_autospec

import pytest
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.lsql.backends import MockBackend, SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.errors.platform import ResourceConflict
from databricks.sdk.service.catalog import TableInfo

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler

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
    ws = create_autospec(WorkspaceClient)
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    installation = MockInstallation()
    table_mapping = TableMapping(installation, ws, backend)

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = []

    with pytest.raises(ValueError):
        list(table_mapping.current_tables(tables_crawler, "a", "b"))

    ws.tables.get.assert_not_called()


def test_current_tables_some_rules():
    ws = create_autospec(WorkspaceClient)
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

    ws.tables.get.assert_not_called()


def test_save_mapping():
    ws = create_autospec(WorkspaceClient)
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

    ws.tables.get.assert_not_called()

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
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = NotFound(...)
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    installation = MockInstallation()
    table_mapping = TableMapping(installation, ws, backend)

    with pytest.raises(ValueError):
        table_mapping.load()

    ws.tables.get.assert_not_called()


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

    ws.tables.get.assert_not_called()

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
    ws.tables.get.assert_not_called()
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
    ws.tables.get.assert_not_called()
    assert [rec.message for rec in caplog.records if "schema not found" in rec.message.lower()]


def test_skip_missing_table(caplog):
    ws = create_autospec(WorkspaceClient)
    sbe = create_autospec(SqlBackend)
    installation = MockInstallation()
    sbe.execute.side_effect = NotFound("[TABLE_OR_VIEW_NOT_FOUND]")
    mapping = TableMapping(installation, ws, sbe)
    mapping.skip_table('foo', table="table")
    ws.tables.get.assert_not_called()
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

    installation = MockInstallation(
        {
            'mapping.csv': [
                {
                    'workspace_name': "fake_ws",
                    "catalog_name": 'cat1',
                    'src_schema': 'schema1',
                    'dst_schema': 'schema1',
                    'src_table': 'table1',
                    'dst_table': 'table1',
                }
            ]
        }
    )
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
    with pytest.raises(ResourceConflict):
        try:
            table_mapping.get_tables_to_migrate(tables_crawler)
        except ManyError as e:
            assert len(e.errs) == 1
            raise e.errs[0]

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
    client = create_autospec(WorkspaceClient)
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
    client = create_autospec(WorkspaceClient)
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


def test_database_not_exists_when_checking_inscope(caplog):
    client = create_autospec(WorkspaceClient)
    tables_crawler = create_autospec(TablesCrawler)
    # When check schema properties for UCX_SKIP_PROPERTY, raise NotFound
    backend = MockBackend(fails_on_first={"DESCRIBE SCHEMA EXTENDED": "SCHEMA_NOT_FOUND"})
    installation = MockInstallation(
        {
            'mapping.csv': [
                {
                    'catalog_name': 'catalog',
                    'dst_schema': 'deleted_schema',
                    'dst_table': 'table',
                    'src_schema': 'deleted_schema',
                    'src_table': 'table',
                    'workspace_name': 'workspace',
                },
            ]
        }
    )
    table_mapping = TableMapping(installation, client, backend)
    table_mapping.get_tables_to_migrate(tables_crawler)
    client.tables.get.assert_not_called()
    tables_crawler.snapshot.assert_called_once()
    assert (
        "Schema hive_metastore.deleted_schema no longer exists. Skipping its properties check and migration."
        in caplog.text
    )


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
    assert not table_mapping.exists_in_uc(src_table, "cat1.schema1.dest1")
    assert table_mapping.exists_in_uc(src_table, "cat1.schema2.dest2")


def test_mapping_broken_table(caplog):
    client = create_autospec(WorkspaceClient)
    tables_crawler = create_autospec(TablesCrawler)
    # When check table properties, raise token error
    backend = MockBackend(fails_on_first={"SHOW TBLPROPERTIES": "tokentoken"})
    installation = MockInstallation(
        {
            'mapping.csv': [
                {
                    'catalog_name': 'catalog',
                    'dst_schema': 'schema1',
                    'dst_table': 'table1',
                    'src_schema': 'schema1',
                    'src_table': 'table1',
                    'workspace_name': 'workspace',
                },
            ]
        }
    )
    client.tables.get.side_effect = NotFound()
    table_mapping = TableMapping(installation, client, backend)
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
    assert "Failed to get properties for Table hive_metastore.schema1.table1" in caplog.text


def test_table_with_no_target_reverted_failed(caplog):
    errors = {"ALTER TABLE": "ALTER_TABLE_FAILED"}
    rows = {
        "SHOW TBLPROPERTIES schema1.table1": [
            {"key": "upgraded_to", "value": "non.existing.table"},
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    client = create_autospec(WorkspaceClient)
    client.tables.get.side_effect = NotFound()

    installation = MockInstallation(
        {
            'mapping.csv': [
                {
                    'workspace_name': "fake_ws",
                    "catalog_name": 'cat1',
                    'src_schema': 'schema1',
                    'dst_schema': 'schema1',
                    'src_table': 'table1',
                    'dst_table': 'table1',
                }
            ]
        }
    )
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
    assert "Failed to unset upgraded_to property" in caplog.text

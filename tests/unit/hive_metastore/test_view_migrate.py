from databricks.labs.ucx.hive_metastore.mapping import Rule
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex, MigrationStatus
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.view_migrate import ViewToMigrate


def test_view_to_migrate_sql_migrate_view():
    expected_query = "CREATE OR REPLACE VIEW IF NOT EXISTS `cat1`.`schema1`.`dest_view1` AS SELECT * FROM `cat1`.`schema1`.`dest_table1`"
    view = Table(
        object_type="VIEW",
        table_format="VIEW",
        catalog="hive_metastore",
        database="test_schema1",
        name="test_view1",
        # The view text is overwritten with the create view statement before running the sql migrate view
        view_text="CREATE OR REPLACE VIEW hive_metastore.test_schema1.test_view1 AS SELECT * FROM test_schema1.test_table1",
    )
    rule = Rule("workspace", "cat1", "test_schema1", "schema1", "test_view1", "dest_view1")
    view_to_migrate = ViewToMigrate(view, rule)
    migration_index = MigrationIndex(
        [
            MigrationStatus("test_schema1", "test_table1", "cat1", "schema1", "dest_table1"),
            MigrationStatus("test_schema1", "test_view1")
        ]
    )

    sql = view_to_migrate.sql_migrate_view(migration_index)

    assert sql == expected_query

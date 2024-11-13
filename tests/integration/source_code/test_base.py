from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.source_code.used_table import UsedTable


def test_used_table_from_table() -> None:
    table = Table("catalog", "schema", "table", "MANAGED", "DELTA")

    used_table = UsedTable.from_table(table, is_read=False, is_write=True)

    assert used_table.catalog_name == "catalog"
    assert used_table.schema_name == "schema"
    assert used_table.table_name == "table"
    assert not used_table.is_read
    assert used_table.is_write

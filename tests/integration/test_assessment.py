from databricks.labs.ucx.toolkits.assessment import Assessment


def test_table_inventory(ws):
    assess = Assessment(ws, "hive_metastore", "uc_assessment")
    assess.table_inventory()
from databricks.labs.ucx.toolkits.assessment import Assessment


def test_table_inventory(ws):
    assess = Assessment(ws, "CSX", "assessment")
    assess.table_inventory()

from databricks.labs.ucx.toolkits.assessment import AssessmentToolkit


def test_table_inventory(ws, make_catalog, make_schema):
    assess = AssessmentToolkit(ws, make_catalog(), make_schema())
    assess.table_inventory()

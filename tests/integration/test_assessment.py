from databricks.labs.ucx.toolkits.assessment import AssessmentToolkit
from databricks.labs.ucx.tacl.tables import Table


def test_table_inventory(ws):
    assess = AssessmentToolkit(ws, "Fake_ID", "CSX", "assessment")
    assess.table_inventory()


def test_external_locations(ws):
    assess = AssessmentToolkit(ws, "Fake_ID", "CSX", "assessment")
    sample_tables = [
        Table("No_Catalog", "No_Database", "No_Name", "TABLE", "DELTA",
              location = "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table"),
        Table("No_Catalog", "No_Database", "No_Name", "TABLE", "DELTA",
              location = "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table2"),
        Table("No_Catalog", "No_Database", "No_Name", "TABLE", "DELTA",
              location = "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/testloc/Table3"),
        Table("No_Catalog", "No_Database", "No_Name", "TABLE", "DELTA",
              location = "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/Table4")
    ]
    result_set = assess.external_locations(sample_tables)
    assert(result_set[0] == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/")
    assert(result_set[1] == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/")
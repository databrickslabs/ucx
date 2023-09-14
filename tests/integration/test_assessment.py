import pytest

from databricks.labs.ucx.assessment.assessment import AssessmentToolkit


def test_table_inventory(ws, make_catalog, make_schema):
    pytest.skip("test is broken")
    assess = AssessmentToolkit(ws, make_catalog(), make_schema())
    assess.table_inventory()

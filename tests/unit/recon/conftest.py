import pytest
from databricks.labs.lsql.backends import MockBackend


@pytest.fixture()
def metadata_row_factory():
    yield MockBackend.rows(
        "col_name",
        "data_type",
    )


@pytest.fixture()
def row_count_row_factory():
    yield MockBackend.rows(
        "row_count",
    )


@pytest.fixture()
def data_comp_row_factory():
    yield MockBackend.rows(
        "total_mismatches",
        "num_missing_records_in_target",
        "num_missing_records_in_source",
    )

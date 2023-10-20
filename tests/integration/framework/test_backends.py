import pytest

from databricks.labs.ucx.assessment.crawlers import JobInfo


def test_sql_backend_with_none_rows_should_be_filtered(sql_backend, inventory_schema):
    rows = [
        JobInfo(
            job_id="754086582578970",
            job_name="sdk-kHJZ",
            creator="dd78bc9b-e2f0-4d26-bdd0-0ad7c3aecc9a",
            success=1,
            failures="[]",
        ),
        None,
    ]
    sql_backend.save_table(full_name=f"hive_metastore.{inventory_schema}.jobs", rows=rows, klass=JobInfo)

    raw = list(sql_backend.fetch(f"SELECT * FROM hive_metastore.{inventory_schema}.jobs"))
    output = []
    for row in raw:
        output.append(JobInfo(*row))
    assert output == [
        JobInfo(
            job_id="754086582578970",
            job_name="sdk-kHJZ",
            creator="dd78bc9b-e2f0-4d26-bdd0-0ad7c3aecc9a",
            success=1,
            failures="[]",
        )
    ]


def test_sql_backend_with_columns_containing_none_should_pass(sql_backend, inventory_schema):
    rows = [
        JobInfo(
            job_id="754086582578970",
            job_name="sdk-kHJZ",
            creator="dd78bc9b-e2f0-4d26-bdd0-0ad7c3aecc9a",
            success=1,
            failures="[]",
        ),
        JobInfo(job_id="996949600090435", job_name="Cleanup", creator=None, success=1, failures="[]"),
    ]
    sql_backend.save_table(full_name=f"hive_metastore.{inventory_schema}.jobs", rows=rows, klass=JobInfo)

    raw = list(sql_backend.fetch(f"SELECT * FROM hive_metastore.{inventory_schema}.jobs"))
    output = []
    for row in raw:
        output.append(JobInfo(*row))
    assert output == rows


def test_sql_backend_with_columns_containing_none_on_not_null_columns_should_fail(sql_backend, inventory_schema):
    rows = [
        JobInfo(
            job_id="754086582578970",
            job_name="sdk-kHJZ",
            creator="dd78bc9b-e2f0-4d26-bdd0-0ad7c3aecc9a",
            success=1,
            failures="[]",
        ),
        JobInfo(job_id=None, job_name="Cleanup", creator="xxx", success=1, failures="[]"),
    ]

    with pytest.raises(RuntimeError):
        sql_backend.save_table(full_name=f"hive_metastore.{inventory_schema}.jobs", rows=rows, klass=JobInfo)

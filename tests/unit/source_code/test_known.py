from databricks.labs.ucx.source_code.known import Whitelist


def test_checks_compatibility():
    known = Whitelist()
    spark_sql = known.compatibility("spark.sql")
    assert not spark_sql.known

    sdk_compute = known.compatibility("databricks.sdk.service.compute")
    assert sdk_compute.known
    assert not sdk_compute.problems

    assert known.compatibility("sys").known

    os_path = known.compatibility("os.path")
    assert os_path.known
    assert not os_path.problems


def test_checks_library_compatibility():
    known = Whitelist()

    sklearn = known.distribution_compatibility("scikit-learn")
    assert sklearn.known
    assert not sklearn.problems

    s3fs = known.distribution_compatibility("s3fs")
    assert s3fs.known
    assert len(s3fs.problems) == 1

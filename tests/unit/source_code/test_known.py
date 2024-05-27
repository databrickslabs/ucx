from databricks.labs.ucx.source_code.known import Whitelist


def test_checks_compatibility():
    wl = Whitelist()
    spark_sql = wl.compatibility("spark.sql")
    assert not spark_sql.known

    sdk_compute = wl.compatibility("databricks.sdk.service.compute")
    assert sdk_compute.known
    assert not sdk_compute.problems

    assert wl.compatibility("sys").known

    os_path = wl.compatibility("os.path")
    assert os_path.known
    assert not os_path.problems


def test_checks_library_compatibility():
    wl = Whitelist()

    sklearn = wl.distribution_compatibility("scikit-learn")
    assert sklearn.known
    assert not sklearn.problems

    s3fs = wl.distribution_compatibility("s3fs")
    assert s3fs.known
    assert len(s3fs.problems) == 1

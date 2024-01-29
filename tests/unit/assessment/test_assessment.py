from databricks.labs.ucx.assessment.crawlers import spark_version_compatibility


def test_spark_version_compatibility():
    assert "unsupported" == spark_version_compatibility("custom:snapshot__14")
    assert "unsupported" == spark_version_compatibility("custom:custom-local__13.x-snapshot-scala2.12__unknown__head__")
    assert "dlt" == spark_version_compatibility("dlt:12.2-delta-pipelines-dlt-release")
    assert "unsupported" == spark_version_compatibility("6.4.x-esr-scala2.11")
    assert "unsupported" == spark_version_compatibility("9.3.x-cpu-ml-scala2.12")
    assert "kinda works" == spark_version_compatibility("10.4.x-scala2.12")
    assert "supported" == spark_version_compatibility("11.3.x-photon-scala2.12")
    assert "unsupported" == spark_version_compatibility("12.2.1-scala2.12")
    assert "supported" == spark_version_compatibility("14.1.x-photon-scala2.12")
    assert "supported" == spark_version_compatibility("123.1.x-quantumscale")
    assert "supported" == spark_version_compatibility("14.1.x-gpu-ml-scala2.12")
    assert "supported" == spark_version_compatibility("14.1.x-cpu-ml-scala2.12")
    assert "unsupported" == spark_version_compatibility("x14.1.x-photon-scala2.12")

from databricks.labs.ucx.assessment.crawlers import spark_version_compatibility, is_mlr


def test_spark_version_compatibility():
    assert spark_version_compatibility("custom:snapshot__14") == "unsupported"
    assert spark_version_compatibility("custom:custom-local__13.x-snapshot-scala2.12__unknown__head__") == "unsupported"
    assert spark_version_compatibility("dlt:12.2-delta-pipelines-dlt-release") == "dlt"
    assert spark_version_compatibility("6.4.x-esr-scala2.11") == "unsupported"
    assert spark_version_compatibility("9.3.x-cpu-ml-scala2.12") == "unsupported"
    assert spark_version_compatibility("10.4.x-scala2.12") == "kinda works"
    assert spark_version_compatibility("11.3.x-photon-scala2.12") == "supported"
    assert spark_version_compatibility("12.2.1-scala2.12") == "unsupported"
    assert spark_version_compatibility("14.1.x-photon-scala2.12") == "supported"
    assert spark_version_compatibility("123.1.x-quantumscale") == "supported"
    assert spark_version_compatibility("14.1.x-gpu-ml-scala2.12") == "supported"
    assert spark_version_compatibility("14.1.x-cpu-ml-scala2.12") == "supported"
    assert spark_version_compatibility("x14.1.x-photon-scala2.12") == "unsupported"
    assert spark_version_compatibility(None) == "unreported version"


def test_is_mlr():
    assert is_mlr("custom:snapshot__14") is False
    assert is_mlr("custom:snapshot__14_ml") is False
    assert is_mlr("custom:custom-local__13.x-snapshot-scala2.12__unknown__head__") is False
    assert is_mlr("dlt:12.2-delta-pipelines-dlt-release") is False
    assert is_mlr("6.4.x-esr-scala2.11") is False
    assert is_mlr("9.3.x-cpu-ml-scala2.12") is True
    assert is_mlr("10.4.x-scala2.12") is False
    assert is_mlr("11.3.x-photon-scala2.12") is False
    assert is_mlr("12.2.1-scala2.12") is False
    assert is_mlr("14.1.x-photon-scala2.12") is False
    assert is_mlr("123.1.x-quantumscale") is False
    assert is_mlr("14.1.x-gpu-ml-scala2.12") is True
    assert is_mlr("14.1.x-cpu-ml-scala2.12") is True
    assert is_mlr("x14.1.x-photon-scala2.12") is False
    assert is_mlr(None) is False

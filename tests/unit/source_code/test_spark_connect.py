from databricks.labs.ucx.source_code.base import Advisory, Failure
from databricks.labs.ucx.source_code.spark_connect import SparkConnectLinter
from databricks.sdk.service.compute import DataSecurityMode


def test_jvm_access_match_shared():
    linter = SparkConnectLinter(data_security_mode=DataSecurityMode.USER_ISOLATION)
    code = """
spark.range(10).collect()
spark._jspark._jvm.com.my.custom.Name()
    """

    assert [
        Failure(
            code="shared-clusters",
            message='Cannot access Spark Driver JVM on UC Shared Clusters',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=18
        ),
        Failure(
            code="shared-clusters",
            message='Cannot access Spark Driver JVM on UC Shared Clusters',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=13
        )
    ] == list(linter.lint(code))


def test_jvm_access_match_non_shared():
    linter = SparkConnectLinter(data_security_mode=DataSecurityMode.SINGLE_USER)
    code = """
spark.range(10).collect()
spark._jspark._jvm.com.my.custom.Name()
    """

    assert [
        Advisory(
            code="shared-clusters",
            message='Cannot access Spark Driver JVM on UC Shared Clusters',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=18
        ),
        Advisory(
            code="shared-clusters",
            message='Cannot access Spark Driver JVM on UC Shared Clusters',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=13
        )
    ] == list(linter.lint(code))


def test_rdd_match_shared():
    linter = SparkConnectLinter(data_security_mode=DataSecurityMode.USER_ISOLATION)
    code = """
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)
    """
    assert [
        Failure(
            code="shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=32
        ),
        Failure(
            code="shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=42
        )
    ] == list(linter.lint(code))


def test_rdd_match_non_shared():
    linter = SparkConnectLinter(data_security_mode=DataSecurityMode.SINGLE_USER)
    code = """
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)
    """
    assert [
        Advisory(
            code="shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=32
        ),
        Advisory(
            code="shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=42
        )
    ] == list(linter.lint(code))


def test_spark_sql_context_matcher_shared():
    linter = SparkConnectLinter(data_security_mode=DataSecurityMode.USER_ISOLATION)
    code = """
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)
    """
    assert [
        Failure(
            code="shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=32
        ),
        Failure(
            code="shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=42
        )
    ] == list(linter.lint(code))


def test_spark_sql_context_matcher_non_shared():
    linter = SparkConnectLinter(data_security_mode=DataSecurityMode.SINGLE_USER)
    code = """
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)
    """
    assert [
        Advisory(
            code="shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=32
        ),
        Advisory(
            code="shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=42
        )
    ] == list(linter.lint(code))


def test_valid_code():
    linter = SparkConnectLinter(data_security_mode=DataSecurityMode.USER_ISOLATION)
    code = """
df = spark.range(10)
df.collect()
    """
    assert [] == list(linter.lint(code))
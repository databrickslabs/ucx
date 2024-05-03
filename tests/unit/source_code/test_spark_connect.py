from databricks.labs.ucx.source_code.base import Failure
from databricks.labs.ucx.source_code.spark_connect import SparkConnectLinter


def test_jvm_access_match_shared():
    linter = SparkConnectLinter(is_serverless=False)
    code = """
spark.range(10).collect()
spark._jspark._jvm.com.my.custom.Name()
    """

    assert [
        Failure(
            code="jvm-access-in-shared-clusters",
            message='Cannot access Spark Driver JVM on UC Shared Clusters',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=18,
        ),
        Failure(
            code="jvm-access-in-shared-clusters",
            message='Cannot access Spark Driver JVM on UC Shared Clusters',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=13,
        ),
    ] == list(linter.lint(code))


def test_jvm_access_match_serverless():
    linter = SparkConnectLinter(is_serverless=True)
    code = """
spark.range(10).collect()
spark._jspark._jvm.com.my.custom.Name()
    """

    assert [
        Failure(
            code="jvm-access-in-shared-clusters",
            message='Cannot access Spark Driver JVM on Serverless Compute',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=18,
        ),
        Failure(
            code="jvm-access-in-shared-clusters",
            message='Cannot access Spark Driver JVM on Serverless Compute',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=13,
        ),
    ] == list(linter.lint(code))


def test_rdd_context_match_shared():
    linter = SparkConnectLinter(is_serverless=False)
    code = """
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)
    """
    assert [
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=32,
        ),
        Failure(
            code='legacy-context-in-shared-clusters',
            message='sc is not supported on UC Shared Clusters. Rewrite it using spark',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=21,
        ),
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=42,
        ),
        Failure(
            code="legacy-context-in-shared-clusters",
            message='sc is not supported on UC Shared Clusters. Rewrite it using spark',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=40,
        ),
    ] == list(linter.lint(code))


def test_rdd_context_match_serverless():
    linter = SparkConnectLinter(is_serverless=True)
    code = """
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)
    """
    assert [
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on Serverless Compute. Rewrite it using DataFrame API',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=32,
        ),
        Failure(
            code='legacy-context-in-shared-clusters',
            message='sc is not supported on Serverless Compute. Rewrite it using spark',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=21,
        ),
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on Serverless Compute. Rewrite it using DataFrame API',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=42,
        ),
        Failure(
            code="legacy-context-in-shared-clusters",
            message='sc is not supported on Serverless Compute. Rewrite it using spark',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=40,
        ),
    ] == list(linter.lint(code))


def test_valid_code():
    linter = SparkConnectLinter()
    code = """
df = spark.range(10)
df.collect()
    """
    assert not list(linter.lint(code))

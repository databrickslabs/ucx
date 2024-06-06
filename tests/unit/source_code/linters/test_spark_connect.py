from itertools import chain

from databricks.labs.ucx.source_code.base import Failure
from databricks.labs.ucx.source_code.linters.python_ast import TreeWalker, Tree
from databricks.labs.ucx.source_code.linters.spark_connect import LoggingMatcher, SparkConnectLinter


def test_jvm_access_match_shared():
    linter = SparkConnectLinter(is_serverless=False)
    code = """
spark.range(10).collect()
spark._jspark._jvm.com.my.custom.Name()
    """
    expected = [
        Failure(
            code="jvm-access-in-shared-clusters",
            message='Cannot access Spark Driver JVM on UC Shared Clusters',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=18,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


def test_jvm_access_match_serverless():
    linter = SparkConnectLinter(is_serverless=True)
    code = """
spark.range(10).collect()
spark._jspark._jvm.com.my.custom.Name()
    """

    expected = [
        Failure(
            code="jvm-access-in-shared-clusters",
            message='Cannot access Spark Driver JVM on Serverless Compute',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=18,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


def test_rdd_context_match_shared():
    linter = SparkConnectLinter(is_serverless=False)
    code = """
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)
    """
    expected = [
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=32,
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
            code='legacy-context-in-shared-clusters',
            message='sc is not supported on UC Shared Clusters. Rewrite it using spark',
            start_line=2,
            start_col=7,
            end_line=2,
            end_col=21,
        ),
        Failure(
            code="legacy-context-in-shared-clusters",
            message='sc is not supported on UC Shared Clusters. Rewrite it using spark',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=40,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


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
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on Serverless Compute. Rewrite it using DataFrame API',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=42,
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
            code="legacy-context-in-shared-clusters",
            message='sc is not supported on Serverless Compute. Rewrite it using spark',
            start_line=3,
            start_col=29,
            end_line=3,
            end_col=40,
        ),
    ] == list(linter.lint(code))


def test_rdd_map_partitions():
    linter = SparkConnectLinter(is_serverless=False)
    code = """
df = spark.createDataFrame([])
df.rdd.mapPartitions(myUdf)
    """
    expected = [
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on UC Shared Clusters. Use mapInArrow() or Pandas UDFs instead',
            start_line=3,
            start_col=0,
            end_line=3,
            end_col=27,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


def test_conf_shared():
    linter = SparkConnectLinter(is_serverless=False)
    code = """df.sparkContext.getConf().get('spark.my.conf')"""
    assert [
        Failure(
            code='legacy-context-in-shared-clusters',
            message='sparkContext and getConf are not supported on UC Shared Clusters. Rewrite it using spark.conf',
            start_line=1,
            start_col=0,
            end_line=1,
            end_col=23,
        ),
    ] == list(linter.lint(code))


def test_conf_serverless():
    linter = SparkConnectLinter(is_serverless=True)
    code = """sc._conf().get('spark.my.conf')"""
    expected = [
        Failure(
            code='legacy-context-in-shared-clusters',
            message='sc and _conf are not supported on Serverless Compute. Rewrite it using spark.conf',
            start_line=1,
            start_col=0,
            end_line=1,
            end_col=8,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


def test_logging_shared():
    logging_matcher = LoggingMatcher(is_serverless=False)
    code = """
sc.setLogLevel("INFO")
setLogLevel("WARN")

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
sc._jvm.org.apache.log4j.LogManager.getLogger(__name__).info("test")

    """

    assert [
        Failure(
            code='spark-logging-in-shared-clusters',
            message='Cannot set Spark log level directly from code on UC Shared Clusters. '
            'Remove the call and set the cluster spark conf \'spark.log.level\' instead',
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=22,
        ),
        Failure(
            code='spark-logging-in-shared-clusters',
            message='Cannot access Spark Driver JVM logger on UC Shared Clusters. ' 'Use logging.getLogger() instead',
            start_line=5,
            start_col=14,
            end_line=5,
            end_col=38,
        ),
        Failure(
            code='spark-logging-in-shared-clusters',
            message='Cannot access Spark Driver JVM logger on UC Shared Clusters. ' 'Use logging.getLogger() instead',
            start_line=7,
            start_col=0,
            end_line=7,
            end_col=24,
        ),
    ] == list(chain.from_iterable([logging_matcher.lint(node) for node in TreeWalker.walk(Tree.parse(code).root)]))


def test_logging_serverless():
    logging_matcher = LoggingMatcher(is_serverless=True)
    code = """
sc.setLogLevel("INFO")
log4jLogger = sc._jvm.org.apache.log4j

    """

    assert [
        Failure(
            code='spark-logging-in-shared-clusters',
            message='Cannot set Spark log level directly from code on Serverless Compute. '
            'Remove the call and set the cluster spark conf \'spark.log.level\' instead',
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=22,
        ),
        Failure(
            code='spark-logging-in-shared-clusters',
            message='Cannot access Spark Driver JVM logger on Serverless Compute. ' 'Use logging.getLogger() instead',
            start_line=3,
            start_col=14,
            end_line=3,
            end_col=38,
        ),
    ] == list(chain.from_iterable([logging_matcher.lint(node) for node in TreeWalker.walk(Tree.parse(code).root)]))


def test_valid_code():
    linter = SparkConnectLinter()
    code = """
df = spark.range(10)
df.collect()
    """
    assert not list(linter.lint(code))

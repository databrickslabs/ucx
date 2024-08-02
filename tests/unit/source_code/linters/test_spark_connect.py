from itertools import chain

import pytest


from databricks.labs.ucx.source_code.base import Failure, CurrentSessionState
from databricks.labs.ucx.source_code.linters.python_ast import Tree
from databricks.labs.ucx.source_code.linters.spark_connect import LoggingMatcher, SparkConnectLinter
from databricks.sdk.service.compute import DataSecurityMode


@pytest.fixture
def session_state() -> CurrentSessionState:
    return CurrentSessionState(data_security_mode=DataSecurityMode.USER_ISOLATION)


def test_jvm_access_match_shared(session_state):
    linter = SparkConnectLinter(session_state)
    code = """
spark.range(10).collect()
spark._jspark._jvm.com.my.custom.Name()
    """
    expected = [
        Failure(
            code="jvm-access-in-shared-clusters",
            message='Cannot access Spark Driver JVM on Unity Catalog clusters in Shared access mode',
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=18,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


def test_jvm_access_match_serverless(session_state):
    session_state.is_serverless = True
    linter = SparkConnectLinter(session_state)
    code = """
spark.range(10).collect()
spark._jspark._jvm.com.my.custom.Name()
    """

    expected = [
        Failure(
            code="jvm-access-in-shared-clusters",
            message='Cannot access Spark Driver JVM on Serverless Compute',
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=18,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


def test_rdd_context_match_shared(session_state):
    linter = SparkConnectLinter(session_state)
    code = """
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)
    """
    expected = [
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on Unity Catalog clusters in Shared access mode. Rewrite it using DataFrame API',
            start_line=1,
            start_col=7,
            end_line=1,
            end_col=32,
        ),
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on Unity Catalog clusters in Shared access mode. Rewrite it using DataFrame API',
            start_line=2,
            start_col=29,
            end_line=2,
            end_col=42,
        ),
        Failure(
            code='legacy-context-in-shared-clusters',
            message='sc is not supported on Unity Catalog clusters in Shared access mode. Rewrite it using spark',
            start_line=1,
            start_col=7,
            end_line=1,
            end_col=21,
        ),
        Failure(
            code="legacy-context-in-shared-clusters",
            message='sc is not supported on Unity Catalog clusters in Shared access mode. Rewrite it using spark',
            start_line=2,
            start_col=29,
            end_line=2,
            end_col=40,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


def test_rdd_context_match_serverless(session_state):
    session_state.is_serverless = True
    linter = SparkConnectLinter(session_state)
    code = """
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)
    """
    assert [
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on Serverless Compute. Rewrite it using DataFrame API',
            start_line=1,
            start_col=7,
            end_line=1,
            end_col=32,
        ),
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on Serverless Compute. Rewrite it using DataFrame API',
            start_line=2,
            start_col=29,
            end_line=2,
            end_col=42,
        ),
        Failure(
            code='legacy-context-in-shared-clusters',
            message='sc is not supported on Serverless Compute. Rewrite it using spark',
            start_line=1,
            start_col=7,
            end_line=1,
            end_col=21,
        ),
        Failure(
            code="legacy-context-in-shared-clusters",
            message='sc is not supported on Serverless Compute. Rewrite it using spark',
            start_line=2,
            start_col=29,
            end_line=2,
            end_col=40,
        ),
    ] == list(linter.lint(code))


def test_rdd_map_partitions(session_state):
    linter = SparkConnectLinter(session_state)
    code = """
df = spark.createDataFrame([])
df.rdd.mapPartitions(myUdf)
    """
    expected = [
        Failure(
            code="rdd-in-shared-clusters",
            message='RDD APIs are not supported on Unity Catalog clusters in Shared access mode. Use mapInArrow() or Pandas UDFs instead',
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=27,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


def test_conf_shared(session_state):
    linter = SparkConnectLinter(session_state)
    code = """df.sparkContext.getConf().get('spark.my.conf')"""
    assert [
        Failure(
            code='legacy-context-in-shared-clusters',
            message='sparkContext and getConf are not supported on Unity Catalog clusters in Shared access mode. Rewrite it using spark.conf',
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=23,
        ),
    ] == list(linter.lint(code))


def test_conf_serverless(session_state):
    session_state.is_serverless = True
    linter = SparkConnectLinter(session_state)
    code = """sc._conf().get('spark.my.conf')"""
    expected = [
        Failure(
            code='legacy-context-in-shared-clusters',
            message='sc and _conf are not supported on Serverless Compute. Rewrite it using spark.conf',
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=8,
        ),
    ]
    actual = list(linter.lint(code))
    assert actual == expected


def test_logging_shared(session_state):
    logging_matcher = LoggingMatcher(session_state)
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
            message='Cannot set Spark log level directly from code on Unity Catalog clusters in Shared access mode. '
            'Remove the call and set the cluster spark conf \'spark.log.level\' instead',
            start_line=1,
            start_col=0,
            end_line=1,
            end_col=22,
        ),
        Failure(
            code='spark-logging-in-shared-clusters',
            message='Cannot access Spark Driver JVM logger on Unity Catalog clusters in Shared access mode. '
            'Use logging.getLogger() instead',
            start_line=4,
            start_col=14,
            end_line=4,
            end_col=38,
        ),
        Failure(
            code='spark-logging-in-shared-clusters',
            message='Cannot access Spark Driver JVM logger on Unity Catalog clusters in Shared access mode. '
            'Use logging.getLogger() instead',
            start_line=6,
            start_col=0,
            end_line=6,
            end_col=24,
        ),
    ] == list(chain.from_iterable([logging_matcher.lint(node) for node in Tree.parse(code).walk()]))


def test_logging_serverless(session_state):
    session_state.is_serverless = True
    logging_matcher = LoggingMatcher(session_state)
    code = """
sc.setLogLevel("INFO")
log4jLogger = sc._jvm.org.apache.log4j

    """

    assert [
        Failure(
            code='spark-logging-in-shared-clusters',
            message='Cannot set Spark log level directly from code on Serverless Compute. '
            'Remove the call and set the cluster spark conf \'spark.log.level\' instead',
            start_line=1,
            start_col=0,
            end_line=1,
            end_col=22,
        ),
        Failure(
            code='spark-logging-in-shared-clusters',
            message='Cannot access Spark Driver JVM logger on Serverless Compute. ' 'Use logging.getLogger() instead',
            start_line=2,
            start_col=14,
            end_line=2,
            end_col=38,
        ),
    ] == list(chain.from_iterable([logging_matcher.lint(node) for node in Tree.parse(code).walk()]))


def test_valid_code():
    linter = SparkConnectLinter(CurrentSessionState(data_security_mode=DataSecurityMode.USER_ISOLATION))
    code = """
df = spark.range(10)
df.collect()
    """
    assert not list(linter.lint(code))

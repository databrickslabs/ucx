# ucx[session-state] {"data_security_mode": "USER_ISOLATION"}
# ucx[legacy-context-in-shared-clusters:+2:0:+2:14] sc is not supported on UC Shared Clusters. Rewrite it using spark
# ucx[spark-logging-in-shared-clusters:+1:0:+1:22] Cannot set Spark log level directly from code on UC Shared Clusters. Remove the call and set the cluster spark conf 'spark.log.level' instead
sc.setLogLevel("INFO")
setLogLevel("WARN")

# ucx[jvm-access-in-shared-clusters:+3:14:+3:21] Cannot access Spark Driver JVM on UC Shared Clusters
# ucx[legacy-context-in-shared-clusters:+2:14:+2:21] sc is not supported on UC Shared Clusters. Rewrite it using spark
# ucx[spark-logging-in-shared-clusters:+1:14:+1:38] Cannot access Spark Driver JVM logger on UC Shared Clusters. Use logging.getLogger() instead
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

# ucx[jvm-access-in-shared-clusters:+3:0:+3:7] Cannot access Spark Driver JVM on UC Shared Clusters
# ucx[legacy-context-in-shared-clusters:+2:0:+2:7] sc is not supported on UC Shared Clusters. Rewrite it using spark
# ucx[spark-logging-in-shared-clusters:+1:0:+1:24] Cannot access Spark Driver JVM logger on UC Shared Clusters. Use logging.getLogger() instead
sc._jvm.org.apache.log4j.LogManager.getLogger(__name__).info("test")

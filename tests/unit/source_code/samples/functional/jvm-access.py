spark.range(10).collect()
# TODO: looks like a bug in linter, because we are hitting the same issue twice
# ucx[jvm-access-in-shared-clusters:+2:0:+2:18] Cannot access Spark Driver JVM on UC Shared Clusters
# ucx[jvm-access-in-shared-clusters:+1:0:+1:18] Cannot access Spark Driver JVM on UC Shared Clusters
spark._jspark._jvm.com.my.custom.Name()

# ucx[jvm-access-in-shared-clusters:+2:0:+2:18] Cannot access Spark Driver JVM on UC Shared Clusters
# ucx[jvm-access-in-shared-clusters:+1:0:+1:18] Cannot access Spark Driver JVM on UC Shared Clusters
spark._jspark._jvm.com.my.custom.Name()

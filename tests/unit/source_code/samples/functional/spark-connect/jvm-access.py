# ucx[session-state] {"data_security_mode": "USER_ISOLATION"}
spark.range(10).collect()
# ucx[jvm-access-in-shared-clusters:+1:0:+1:18] Cannot access Spark Driver JVM on Unity Catalog clusters in Shared access mode
spark._jspark._jvm.com.my.custom.Name()

# ucx[jvm-access-in-shared-clusters:+1:0:+1:18] Cannot access Spark Driver JVM on Unity Catalog clusters in Shared access mode
spark._jspark._jvm.com.my.custom.Name()

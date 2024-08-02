# ucx[session-state] {"data_security_mode": "USER_ISOLATION"}
df = spark.createDataFrame([])
# ucx[rdd-in-shared-clusters:+1:0:+1:27] RDD APIs are not supported on Unity Catalog clusters in Shared access mode. Use mapInArrow() or Pandas UDFs instead
df.rdd.mapPartitions(myUdf)

# ucx[rdd-in-shared-clusters:+2:7:+2:32] RDD APIs are not supported on Unity Catalog clusters in Shared access mode. Rewrite it using DataFrame API
# ucx[legacy-context-in-shared-clusters:+1:7:+1:21] sc is not supported on Unity Catalog clusters in Shared access mode. Rewrite it using spark
rdd1 = sc.parallelize([1, 2, 3])

# ucx[rdd-in-shared-clusters:+2:29:+2:42] RDD APIs are not supported on Unity Catalog clusters in Shared access mode. Rewrite it using DataFrame API
# ucx[legacy-context-in-shared-clusters:+1:29:+1:40] sc is not supported on Unity Catalog clusters in Shared access mode. Rewrite it using spark
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)

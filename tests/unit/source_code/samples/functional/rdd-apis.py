df = spark.createDataFrame([])
# ucx[rdd-in-shared-clusters:+1:0:+1:27] RDD APIs are not supported on UC Shared Clusters. Use mapInArrow() or Pandas UDFs instead
df.rdd.mapPartitions(myUdf)

# ucx[rdd-in-shared-clusters:+1:7:+1:32] RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API
# ucx[legacy-context-in-shared-clusters:+1:7:+1:21] sc is not supported on UC Shared Clusters. Rewrite it using spark
rdd1 = sc.parallelize([1, 2, 3])

# ucx[rdd-in-shared-clusters:+1:29:+1:42] RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API
# ucx[legacy-context-in-shared-clusters:+1:29:+1:40] sc is not supported on UC Shared Clusters. Rewrite it using spark
# ucx[rdd-in-shared-clusters:+1:0:+1:24] RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API
rdd2 = spark.createDataFrame(sc.emptyRDD(), schema)


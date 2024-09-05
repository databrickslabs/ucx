# ucx[direct-filesystem-access:+1:6:+1:26] The use of direct filesystem references is deprecated: dbfs:/mnt/foo/bar1
DBFS1="dbfs:/mnt/foo/bar1"
# ucx[direct-filesystem-access:+1:16:+1:36] The use of direct filesystem references is deprecated: dbfs:/mnt/foo/bar2
systems=[DBFS1, "dbfs:/mnt/foo/bar2"]
for system in systems:
    # ucx[direct-filesystem-access:+2:4:+2:30] The use of direct filesystem references is deprecated: dbfs:/mnt/foo/bar1
    # ucx[direct-filesystem-access:+1:4:+1:30] The use of direct filesystem references is deprecated: dbfs:/mnt/foo/bar2
    spark.read.parquet(system)

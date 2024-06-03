# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
df = spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df.write.format("delta").saveAsTable("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    df.write.format("delta").saveAsTable("table.we.know.nothing.about")

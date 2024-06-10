# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):
    # ucx[table-migrate:+1:23:+1:49] Table old.things is migrated to brand.new.stuff in Unity Catalog
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))

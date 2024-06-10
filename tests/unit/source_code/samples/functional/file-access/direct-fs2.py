# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):
    # ucx[table-migrate:+1:23:+1:49] Table old.things is migrated to brand.new.stuff in Unity Catalog
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
# ucx[table-migrate:+2:0:+2:41] Can't migrate table_name argument in 'spark.sql(f'SELECT * FROM table_{index}')' because its value cannot be computed
index = 10
spark.sql(f"SELECT * FROM table_{index}").collect()
# ucx[table-migrate:+2:14:+2:40] Table old.things is migrated to brand.new.stuff in Unity Catalog
# ucx[table-migrate:+2:4:+2:20] Can't migrate table_name argument in 'spark.sql(query)' because its value cannot be computed
for query in ["SELECT * FROM old.things", f"SELECT * FROM table_{index}"]:
    spark.sql(query).collect()

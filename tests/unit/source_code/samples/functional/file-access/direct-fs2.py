# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):
    # ucx[table-migrate:+1:13:+1:50] Table old.things is migrated to brand.new.stuff in Unity Catalog
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
index = 10
spark.sql(f"SELECT * FROM table_{index}").collect()
# ucx[table-migrate:+2:0:+2:40] Can't migrate table_name argument in 'spark.sql(f'SELECT * FROM {table_name}')' because its value cannot be computed
table_name = f"table_{index}"
spark.sql(f"SELECT * FROM {table_name}").collect()
# ucx[table-migrate:+4:4:+4:20] Table old.things is migrated to brand.new.stuff in Unity Catalog
# ucx[table-migrate:+3:4:+3:20] Can't migrate table_name argument in 'spark.sql(query)' because its value cannot be computed
table_name = f"table_{index}"
for query in ["SELECT * FROM old.things", f"SELECT * FROM {table_name}"]:
    spark.sql(query).collect()

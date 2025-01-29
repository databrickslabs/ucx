# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):
    # ucx[table-migrated-to-uc-python-sql:+1:13:+1:50] Table old.things is migrated to brand.new.stuff in Unity Catalog
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
index = 10
spark.sql(f"SELECT * FROM table_{index}").collect()
table_name = f"table_{index}"
spark.sql(f"SELECT * FROM {table_name}").collect()
# ucx[table-migrated-to-uc-python-sql:+3:4:+3:20] Table old.things is migrated to brand.new.stuff in Unity Catalog
table_name = f"table_{index}"
for query in ["SELECT * FROM old.things", f"SELECT * FROM {table_name}"]:
    spark.sql(query).collect()

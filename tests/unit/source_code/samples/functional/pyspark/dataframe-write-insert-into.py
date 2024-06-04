# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
df = spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df.write.insertInto("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    # TODO: Fix false positive:
    # ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df.write.insertInto("table.we.know.nothing.about")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    # TODO: Fix false positive:
    # ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df.write.insertInto("old.things", None, "extra-argument")

    ## Check a call with an out-of-position named argument referencing a table known to be migrated.
    # ucx[table-migrate:+3:0:+3:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    # TODO: Fix false positive:
    # ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df.write.insertInto(overwrite=None, tableName="old.things")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+3:0:+3:0] Can't migrate 'insertInto' because its table name argument is not a constant
    # TODO: Fix false positive:
    # ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df.write.insertInto(name)
    # ucx[table-migrate:+3:0:+3:0] Can't migrate 'insertInto' because its table name argument is not a constant
    # TODO: Fix false positive:
    # ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df.write.insertInto(f"boop{stuff}")

# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+3:0:+3:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    # TODO: Implement missing migration warning (on the source argument):
    # #ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.catalog.createExternalTable("old.things")
    do_stuff_with(df)

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    # TODO: Implement missing migration warning (on the source argument):
    # #ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.catalog.createExternalTable("table.we.know.nothing.about")
    do_stuff_with(df)

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    # FIXME: This is a false positive due to an error in the matching specification; only 4 positional args are allowed.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df = spark.catalog.createExternalTable("old.things", None, None, None, "extra-argument")
    do_stuff_with(df)

    ## Check a call with an out-of-position named argument referencing a table known to be migrated.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df = spark.catalog.createExternalTable(path="foo", tableName="old.things", source="delta")
    do_stuff_with(df)

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+1:0:+1:0] Can't migrate 'createExternalTable' because its table name argument is not a constant
    df = spark.catalog.createExternalTable(name)
    do_stuff_with(df)
    # ucx[table-migrate:+1:0:+1:0] Can't migrate 'createExternalTable' because its table name argument is not a constant
    df = spark.catalog.createExternalTable(f"boop{stuff}")
    do_stuff_with(df)


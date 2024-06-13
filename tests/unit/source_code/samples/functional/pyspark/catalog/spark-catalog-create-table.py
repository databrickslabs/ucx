# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+3:9:+3:48] Table old.things is migrated to brand.new.stuff in Unity Catalog
    # TODO: Implement missing migration warning (on the source argument):
    # #ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.catalog.createTable("old.things")
    do_stuff_with(df)

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    # TODO: Implement missing migration warning (on the source argument):
    # #ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.catalog.createTable("table.we.know.nothing.about")
    do_stuff_with(df)

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    # FIXME: This is a false positive due to an error in the matching specification; only 5 positional args are allowed.
    # ucx[table-migrate:+1:9:+1:90] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df = spark.catalog.createTable("old.things", None, None, None, None, "extra-argument")
    do_stuff_with(df)

    ## Check a call with an out-of-position named argument referencing a table known to be migrated.
    # ucx[table-migrate:+1:9:+1:86] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df = spark.catalog.createTable(path="foo", tableName="old.things", source="delta")
    do_stuff_with(df)

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+1:9:+1:40] Can't migrate 'spark.catalog.createTable(name)' because its table name argument cannot be computed
    df = spark.catalog.createTable(name)
    do_stuff_with(df)
    # ucx[table-migrate:+1:9:+1:50] Can't migrate 'spark.catalog.createTable(f'boop{stuff}')' because its table name argument cannot be computed
    df = spark.catalog.createTable(f"boop{stuff}")
    do_stuff_with(df)

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    # FIXME: This is a false positive; any method named 'createTable' is triggering the warning.
    # ucx[table-migrate:+1:4:+1:44] Table old.things is migrated to brand.new.stuff in Unity Catalog
    something_else.createTable("old.things")
    a_function("old.things")
